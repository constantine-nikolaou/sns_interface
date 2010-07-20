require 'rubygems'
require 'right_aws'
$:.unshift(File.dirname(__FILE__))
require 'topic'
require 'parsers'

class SnsInterface < RightAwsBase
  include RightAwsBaseInterface
  
  API_VERSION       = "2010-03-31"
  DEFAULT_HOST      = "sns.us-east-1.amazonaws.com" #TODO find the generic endpoint
  DEFAULT_PORT      = 443
  DEFAULT_PROTOCOL  = 'https'
  
  def initialize(aws_access_key_id=nil, aws_secret_access_key=nil, params={})
    init({ :name             => 'SNS', 
           :default_host     => ENV['SNS_URL'] ? URI.parse(ENV['SNS_URL']).host   : DEFAULT_HOST, 
           :default_port     => ENV['SNS_URL'] ? URI.parse(ENV['SNS_URL']).port   : DEFAULT_PORT, 
           :default_protocol => ENV['SNS_URL'] ? URI.parse(ENV['SNS_URL']).scheme : DEFAULT_PROTOCOL }, 
         aws_access_key_id     || ENV['AWS_ACCESS_KEY_ID'], 
         aws_secret_access_key || ENV['AWS_SECRET_ACCESS_KEY'], 
         params)
  end
  
  #-----------------------------------------------------------------
  #      Requests
  #-----------------------------------------------------------------

  # Generates a request hash for the query API
  def generate_request(action, param={})  # :nodoc:
    param.each{ |key, value| param.delete(key) if (value.nil? || key.is_a?(Symbol)) }
    
    service_hash = { "Action" => action,"AWSAccessKeyId" => @aws_access_key_id }
    
    service_hash.update(param)
    service_params = signed_service_params(@aws_secret_access_key, service_hash, :get, @params[:server], '/')
    request        = Net::HTTP::Get.new("/?#{service_params}")
      # prepare output hash
    { :request  => request,
      :server   => @params[:server],
      :port     => @params[:port],
      :protocol => @params[:protocol] }
  end
  
  # Sends request to Amazon and parses the response
  # Raises AwsError if anything happens
  def request_info(request, parser) # :nodoc:
    request_info_impl(:sns_connection, @@bench, request, parser)
  end
  
  def publish(topic_arn, message, subject = nil)
    req_hash = generate_request('Publish', 'TopicArn' => topic_arn, 'Message' => message, 'Subject' => subject)
    request_info(req_hash, SnsPublishParser.new(:logger => @logger))
  rescue
    on_exception
  end
  
  def list_subscriptions(next_token = nil)
    req_hash = generate_request('ListSubscriptions', 'NextToken' => next_token)
    request_info(req_hash, SnsListSubscriptionsParser.new(:logger => @logger))
  rescue
    on_exception
  end
  
  def list_subscriptions_by_topic(topic_arn, next_token = nil)
    req_hash = generate_request('ListSubscriptionsByTopic', 'TopicArn' => topic_arn, 'NextToken' => next_token)
    request_info(req_hash, SnsListSubscriptionsParser.new(:logger => @logger))
  rescue
    on_exception
  end
  
  def subscribe(topic_arn, protocol, endpoint)
    req_hash = generate_request('Subscribe', 'TopicArn' => topic_arn, 
                                'Protocol' => protocol,
                                'Endpoint' => endpoint)
    request_info(req_hash, SnsSubscribeParser.new(:logger => @logger))
  rescue
    on_exception
  end
  
  def unsubscribe(subscription_arn)
    req_hash = generate_request('Unsubscribe', 'SubscriptionArn' => subscription_arn)
    request_info(req_hash, SnsStatusParser.new(:logger => @logger))
  rescue
    on_exception
  end
end