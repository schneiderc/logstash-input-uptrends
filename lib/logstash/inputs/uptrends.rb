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

  # If undefined, Logstash will complain, even if codec is unused.
  default :codec, "json"

  config :url_template, :validate => :string, :default => "/probes"

  config :parameters, :validate => :hash, :default => {}

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
                 :url_template => @url_template, :parameters => @parameters, :schedule => @schedule)
  end

  def run(queue)
    raise LogStash::ConfigurationError, "Invalid config. No schedule was specified." unless @schedule
    raise LogStash::ConfigurationError, "Invalid config. URL template seems to be incorrect." unless validate_url
    setup_schedule(queue)
  end

  # def run

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
    started = Time.now

    url = build_url

    @logger.debug? && @logger.debug("Fetching URL", :url => url)

    req_opts = Hash.new

    # TODO add auth config
    req_opts[:auth] = {user: "username", password: "password", eager: true}

    client.parallel.get(url, req_opts).
        on_success {|response| handle_success(queue, url, response, Time.now - started)}.
        on_failure {|exception| handle_failure(queue, url, exception, Time.now - started)
    }

    client.execute!
  end

  def stop
    @scheduler.stop if @scheduler
  end

  private
  def handle_success(queue, url, response, execution_time)
    @logger.debug? && @logger.debug("Response received", :code => response.code.to_s)
    body = response.body
    # If there is a usable response. HEAD requests are `nil` and empty get
    # responses come up as "" which will cause the codec to not yield anything
    if body && body.size > 0
      @codec.decode(body) do |decoded|
        event = @target ? LogStash::Event.new(@target => decoded.to_hash) : LogStash::Event.new("message" => decoded)
        handle_decoded_event(queue, url, response, event, execution_time)
      end
    else
      event = ::LogStash::Event.new
      handle_decoded_event(queue, url, response, event, execution_time)
    end
  end

  private
  # Beware, on old versions of manticore some uncommon failures are not handled
  def handle_failure(queue, url, exception, execution_time)
    @logger.debug? && @logger.debug("Request failed", :exception => exception.to_s)
    event = LogStash::Event.new
    apply_metadata(event, url)

    # This is also in the metadata, but we send it anyone because we want this
    # persisted by default, whereas metadata isn't. People don't like mysterious errors
    event.set("http_request_failure", {
        "url" => url,
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
  def handle_decoded_event(queue, url, response, event, execution_time)
    apply_metadata(event, url, response, execution_time)
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
  def apply_metadata(event, url, response=nil, execution_time=nil)
    return unless @metadata_target
    event.set(@metadata_target, event_metadata(url, response, execution_time))
  end

  private
  def event_metadata(url, response=nil, execution_time=nil)
    m = {
        "host" => @host,
        "url" => url
    }

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
  def build_url
    today = Date.today
    url = "https://api.uptrends.com/v3/" << String.new(@url_template)

    param_values = Hash.new

    @parameters.each_key {|k|
      k_sym = case k
                when Symbol
                  k
                when String
                  k.to_sym
                else
                  raise LogStash::ConfigurationError, "Invaild parameter '" << k.to_s << "'"
              end
      param_values[k_sym] = case @parameters[k].to_sym
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
                                @parameters[k]
                            end
    }

    param_values[:format] = "json"

    url << "?" << URI.encode_www_form(format_params(param_values))
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
  def validate_url
    matches_pattern(@url_template, /\A(probes|probegroups|checkpointservers)(\/[\d\w]{32}\/.*)?\z/)
  end

  private
  def matches_pattern(string, pattern)
    !(string !~ pattern)
  end
end # class LogStash::Inputs::Uptrends
