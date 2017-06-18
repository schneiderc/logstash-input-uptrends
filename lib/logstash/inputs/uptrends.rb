# encoding: utf-8
require 'date'
require "logstash/inputs/base"
require "logstash/namespace"
require "logstash/plugin_mixins/http_client"
require "stud/interval"
require "socket" # for Socket.gethostname
require "manticore"

# Generate a repeating message.
#
# This plugin is intented only as an example.

class LogStash::Inputs::Uptrends < LogStash::Inputs::Base
  include LogStash::PluginMixins::HttpClient

  config_name "uptrends"

  DATE_PARAMS = [
      :today,
      :yesterday,

      # MONTH - CURRENT
      :first_day_of_current_month,
      :last_day_of_current_month,

      # MONTH - PREVIOUS
      :first_day_of_previous_month,
      :last_day_of_previous_month,

      # WEEK - CURRENT
      :monday_of_current_week,
      :tuesday_of_current_week,
      :wednesday_of_current_week,
      :thursday_of_current_week,
      :friday_of_current_week,
      :saturday_of_current_week,
      :sunday_of_current_week,

      # WEEK - PREVIOUS
      :monday_of_previous_week,
      :tuesday_of_previous_week,
      :wednesday_of_previous_week,
      :thursday_of_previous_week,
      :friday_of_previous_week,
      :saturday_of_previous_week,
      :sunday_of_previous_week,

      # DATEPARTS
      :current_day_of_month,
      :current_month,
      :current_year,
      :previous_month
  ]

  # If undefined, Logstash will complain, even if codec is unused.
  default :codec, "json"

  # The message string to use in the event.
  config :message, :validate => :string, :default => "Hello world!"

  # Set how frequently messages should be sent.
  #
  # The default, `1`, means send a message every second.
  config :interval, :validate => :number, :default => 3

  config :parameters, :validate => :hash, :default => {}

  config :url_template, :validate => :string, :default => "/probes"

  # config :time_range, :validate => time_ranges.map{ |range| range.to_s }, :default => :current_month

  public
  def register
    @host = Socket.gethostname
  end

  # def register

  def run(queue)
    # we can abort the loop if stop? becomes true
    while !stop?
      # id = rand(100)
      # url = "https://jsonplaceholder.typicode.com/posts/" + id.to_s
      # response_body = client.get(url).body
      #event = LogStash::Event.new("message" => "Resonse: " + response_body.to_s, "host" => @host)

      url = populate_url_template
      event = LogStash::Event.new("message" => url, "host" => @host)

      decorate(event)
      queue << event
      # because the sleep interval can be big, when shutdown happens
      # we want to be able to abort the sleep
      # Stud.stoppable_sleep will frequently evaluate the given block
      # and abort the sleep(@interval) if the return value is true
      Stud.stoppable_sleep(@interval) {stop?}
    end # loop
  end

  # def run

  def stop
    # nothing to do in this case so it is not necessary to define stop
    # examples of common "stop" tasks:
    #  * close sockets (unblocking blocking reads/accepts)
    #  * cleanup temporary files
    #  * terminate spawned threads
  end

  private
  def handle_success(queue, name, request, response, execution_time)
  end

  private
  # Beware, on old versions of manticore some uncommon failures are not handled
  def handle_failure(queue, name, request, exception, execution_time)

  end

  private
  def populate_url_template
    today = Date.today

    param_values = LogStash::Inputs::Uptrends::DATE_PARAMS.each.with_object({}) do |param, param_values|
      param_values[param] = case param
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
                                # TODO error handling
                                Date.new(1970, 1, 1)
                            end
    end

    # param_values.merge!(symbolized_params(@parameters))

    @url_template % symbolized_params(param_values.merge(@parameters))
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
  def symbolized_params(parameters)
    parameters.inject({}) do |hash,(k,v)|
      case v
        when Date
          hash[k.to_sym] = v.strftime('%Y/%m/%d')
        else
          hash[k.to_sym] = v
      end
      hash
    end
  end

end # class LogStash::Inputs::Uptrends
