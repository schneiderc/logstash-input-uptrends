# encoding: utf-8
require 'date'
require "logstash/inputs/base"
require "logstash/namespace"
require "logstash/plugin_mixins/http_client"
require "socket" # for Socket.gethostname
require "manticore"
require "rufus/scheduler"

# Generate a repeating message.
#
# This plugin is intented only as an example.

class LogStash::Inputs::Uptrends < LogStash::Inputs::Base
  include LogStash::PluginMixins::HttpClient

  config_name "uptrends"

  SCHEDULE_TYPES = %w(cron every at in)

  BASE_URL = "https://api.uptrends.com/v3/"

  # If undefined, Logstash will complain, even if codec is unused.
  default :codec, "json"

  config :operations, :validate => :hash, :required => true

  config :auth, :validate => :hash, :required => true

  # Schedule of when to periodically poll from the urls
  # Format: A hash with
  #   + key: "cron" | "every" | "in" | "at"
  #   + value: string
  # Examples:
  #   a) { "every" => "1h" }
  #   b) { "cron" => "* * * * * UTC" }
  # See: rufus/scheduler for details about different schedule options and value string format
  config :schedule, :validate => :hash

  # Define the target field for placing the received data. If this setting is omitted, the data will be stored at the root (top level) of the event.
  config :target, :validate => :string

  # If you'd like to work with the request/response metadata.
  # Set this value to the name of the field you'd like to store a nested
  # hash of metadata.
  config :metadata_target, :validate => :string, :default => '@metadata'

  public
  def register
    @host = Socket.gethostname
    @logger.info("Registering uptrends Input", :type => @type,
                 :operations => @operations, :schedule => @schedule)

    normalize_operations!
  end

  def run(queue)
    raise LogStash::ConfigurationError, "Invalid config. No schedule was specified." unless @schedule
    setup_schedule(queue)
  end

  # def run

  private
  def normalize_operations!
    normalize_auth!
    @normed_operations = Hash[@operations.map {|name, operation| [name.to_sym, normalize_operation(name, operation)]}]
  end

  private
  def normalize_auth!
    @auth = Hash[@auth.map {|k, v| [k.to_sym, v]}] # symbolize key

    if @auth[:user].nil? || @auth[:password].nil?
      raise LogStash::ConfigurationError, "'user' and 'password' must both be specified within auth!"
    end
  end

  private
  def normalize_operation(name, operation)
    nop = Hash.new

    if operation.is_a?(String)
      url = operation
    elsif operation.is_a?(Hash)
      nop = Hash[operation.map {|k, v| [k.to_sym, v]}] # symbolize key
      url = nop.delete(:path)
    else
      raise LogStash::ConfigurationError, "Invalid operations specification: '#{operation}', expected a String or Hash!"
    end

    nop[:path] = trans_validate_url(name, url)

    validate_operation(name, nop)
    nop
  end

  private
  # Transforms and validates the url
  def trans_validate_url(name, url)
    raise LogStash::ConfigurationError, "No URL provided for operation #{name}" if url.nil? || url.length == 0

    url.gsub!(Regexp.new(BASE_URL), "")

    unless matches_pattern(url, /\A(\/)?(probes|probegroups|checkpointservers)(\/[\d\w]{32}\/.*)?\z/)
      raise LogStash::ConfigurationError, "Invalid URL Invalid URL #{url} for operation #{name}"
    end

    url.gsub!(/\A\//, '')
    url
  end

  private
  def validate_operation(name, nop)
    nop.each_key {|key|
      unless [:path, :parameters, :type].include?(key)
        raise LogStash::ConfigurationError, "Unknown key #{key} for operation #{name}"
      end
    }

    raise LogStash::ConfigurationError, "Parameters for operation #{name} is not a Hash!" unless nop[:parameters].is_a?(Hash)
  end

  private
  def setup_schedule(queue)
    #schedule hash must contain exactly one of the allowed keys
    msg_invalid_schedule = "Invalid config. schedule hash must contain " +
        "exactly one of the following keys - cron, at, every or in"
    raise Logstash::ConfigurationError, msg_invalid_schedule if @schedule.keys.length !=1
    schedule_type = @schedule.keys.first
    schedule_value = @schedule[schedule_type]
    raise LogStash::ConfigurationError, msg_invalid_schedule unless SCHEDULE_TYPES.include?(schedule_type)

    @scheduler = Rufus::Scheduler.new(:max_work_threads => 1)
    #as of v3.0.9, :first_in => :now doesn't work. Use the following workaround instead
    opts = schedule_type == "every" ? {:first_in => 0.01} : {}
    @scheduler.send(schedule_type, schedule_value, opts) {run_once(queue)}
    @scheduler.join
  end

  def run_once(queue)
    @normed_operations.each do |name, operation|
      request_async(queue, name, operation)
    end

    client.execute!
  end

  def stop
    @scheduler.stop if @scheduler
  end

  private
  def request_async(queue, name, operation)
    started = Time.now

    request = build_request(operation[:path], operation[:parameters])

    log_request(request)

    url = request.delete(:url)

    #replace placeholder-parameters with runtime data
    operation[:query] = request[:query]

    client.parallel.get(url, request).
        on_success {|response| handle_success(queue, name, operation, response, Time.now - started)}.
        on_failure {|exception| handle_failure(queue, name, operation, exception, Time.now - started)
    }
  end

  private
  def handle_success(queue, name, operation, response, execution_time)
    @logger.debug? && @logger.debug("Response received", :code => response.code.to_s)
    body = response.body

    # If there is a usable response. HEAD requests are `nil` and empty get
    # responses come up as "" which will cause the codec to not yield anything
    if body && body.size > 0
      @codec.decode(body) do |decoded|
        event = @target ? LogStash::Event.new(@target => decoded.to_hash) : decoded
        handle_decoded_event(queue, name, operation, response, event, execution_time)
      end
    else
      event = ::LogStash::Event.new
      handle_decoded_event(queue, name, operation, response, event, execution_time)
    end
  end

  private
  # Beware, on old versions of manticore some uncommon failures are not handled
  def handle_failure(queue, name, operation, exception, execution_time)
    @logger.debug? && @logger.debug("Request failed", :exception => exception.to_s)
    event = LogStash::Event.new
    apply_metadata(event, name, operation)

    # This is also in the metadata, but we send it anyone because we want this
    # persisted by default, whereas metadata isn't. People don't like mysterious errors
    event.set("http_request_failure", {
        "url" => operation[:path],
        "error" => exception.to_s,
        "backtrace" => exception.backtrace,
        "runtime_seconds" => execution_time
    })

    event.tag("_http_request_failure")
    queue << event
  rescue StandardError, java.lang.Exception => e
    @logger.error? && @logger.error("Cannot read URL or send the error as an event!",
                                    :exception => e,
                                    :exception_message => e.message,
                                    :exception_backtrace => e.backtrace,
                                    :url => url
    )
  end

  private
  def handle_decoded_event(queue, name, operation, response, event, execution_time)
    # set event type
    type = operation[:type].to_s
    event.set("type", type) unless type.nil? || type.length == 0

    apply_metadata(event, name, operation, response, execution_time)
    decorate(event)
    queue << event
  rescue StandardError, java.lang.Exception => e
    @logger.error? && @logger.error("Error eventifying response!",
                                    :exception => e,
                                    :exception_message => e.message,
                                    :response => response
    )
  end

  private
  def apply_metadata(event, name, operation, response=nil, execution_time=nil)
    return unless @metadata_target
    event.set(@metadata_target, event_metadata(name, operation, response, execution_time))
  end

  private
  def event_metadata(name, operation, response=nil, execution_time=nil)

    m = {
        "host" => @host,
        "url" => operation[:path],
        "name" => name
    }

    operation_params = operation[:query]

    unless operation_params.nil?
      operation_params.map {|k, v| [k.to_s, v]}
      m["parameters"] = operation_params
    end

    m["runtime_seconds"] = execution_time

    if response
      m["code"] = response.code
      m["response_headers"] = response.headers
      m["response_message"] = response.message
      m["times_retried"] = response.times_retried
    end

    m
  end

  private
  def build_request(operation_path, parameters = {})
    today = Date.today

    request = Hash.new
    request_params = Hash.new

    unless parameters.nil?
      parameters.each_key {|k|
        k_sym = case k
                  when Symbol
                    k
                  when String
                    k.to_sym
                  else
                    raise LogStash::ConfigurationError, "Invaild parameter '" << k.to_s << "'"
                end
        request_params[k_sym] = case parameters[k].to_sym
                                  when :today
                                    today
                                  when :yesterday
                                    today - 1
                                  when :first_day_of_current_month
                                    Date.new(today.year, today.month, 1)
                                  when :last_day_of_current_month
                                    day_of_different_month(today, 1, 1) - 1
                                  when :first_day_of_previous_month
                                    day_of_different_month(today, -1, 1)
                                  when :last_day_of_previous_month
                                    Date.new(today.year, today.month, 1) - 1
                                  when :monday_of_current_week
                                    day_of_week(today, 1)
                                  when :tuesday_of_current_week
                                    day_of_week(today, 2)
                                  when :wednesday_of_current_week
                                    day_of_week(today, 3)
                                  when :thursday_of_current_week
                                    day_of_week(today, 4)
                                  when :friday_of_current_week
                                    day_of_week(today, 5)
                                  when :saturday_of_current_week
                                    day_of_week(today, 6)
                                  when :sunday_of_current_week
                                    day_of_week(today, 7)
                                  when :monday_of_previous_week
                                    day_of_previous_week(today, 1)
                                  when :tuesday_of_previous_week
                                    day_of_previous_week(today, 2)
                                  when :wednesday_of_previous_week
                                    day_of_previous_week(today, 3)
                                  when :thursday_of_previous_week
                                    day_of_previous_week(today, 4)
                                  when :friday_of_previous_week
                                    day_of_previous_week(today, 5)
                                  when :saturday_of_previous_week
                                    day_of_previous_week(today, 6)
                                  when :sunday_of_previous_week
                                    day_of_previous_week(today, 7)
                                  when :current_day_of_month
                                    today.strftime('%d')
                                  when :current_month
                                    today.strftime('%m')
                                  when :current_year
                                    today.strftime('%Y')
                                  when :previous_month
                                    day_of_different_month(today, -1, 1).strftime('%m')
                                  else
                                    parameters[k]
                                end
      }
    end

    request_params[:format] = "json"

    request[:url] = BASE_URL << String.new(operation_path)
    request[:query] = format_params(request_params)
    request[:auth] = {
        user: @auth[:user],
        password: @auth[:password],
        eager: true
    }

    request
  end

  private
  def day_of_week(date, day_of_week)
    date + day_of_week - date.cwday
  end

  private
  def day_of_previous_week(date, day_of_week)
    day_of_week(date, day_of_week) - 7
  end

  private
  def same_day_of_different_month(date, month_delta)
    if month_delta < 0
      ((date + 1) << -month_delta) - 1
    else
      ((date + 1) >> month_delta) - 1
    end
  end

  private
  def day_of_different_month(date, month_delta, day)
    new_date = same_day_of_different_month(date, month_delta)

    Date.new(new_date.year, new_date.month, day)
  end

  private
  def format_params(parameters)
    parameters.inject({}) do |hash, (k, v)|
      case v
        when Date
          hash[k.to_sym] = v.strftime('%Y/%m/%d')
        else
          hash[k.to_sym] = v
      end
      hash
    end
  end

  private
  def matches_pattern(string, pattern)
    !(string !~ pattern)
  end

  private
  def log_request(request)
    if @logger.debug?
      masked_request = Hash.new

      request.each_key{|key|
        if key == :auth
          auth = request[key]
          v = { user: auth[:user], password: auth[:password].gsub(/./, "*")}
        else
          v = request[key]
        end
        masked_request[key] = v
      }

      @logger.debug("Sending request", :request => masked_request)
    end
  end
end # class LogStash::Inputs::Uptrends
