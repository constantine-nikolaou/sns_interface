$:.unshift(File.dirname(__FILE__)) unless
  $:.include?(File.dirname(__FILE__)) || $:.include?(File.expand_path(File.dirname(__FILE__)))

require 'rubygems'
$:.unshift(File.dirname(__FILE__))
require 'vendor/right_aws-2.0.0/lib/right_aws.rb'
require 'vendor/right_aws-2.0.0/lib/awsbase/right_awsbase.rb'

module SnsInterface
  VERSION = '0.0.1'
end

module RightAws
  class SnsInterface < RightAwsBase
    include RightAwsBaseInterface
    
    API_VERSION       = "2010-03-31"
    DEFAULT_HOST      = "sns.us-east-1.amazonaws.com" #TODO find the generic endpoint
    DEFAULT_PORT      = 443
    DEFAULT_PROTOCOL  = 'https'
    
    @@bench = AwsBenchmarkingBlock.new
    def self.bench_xml
      @@bench.xml
    end
    def self.bench_sns
      @@bench.service
    end

    @@api = API_VERSION
    def self.api 
      @@api
    end
    
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
      service_hash = { "Action"           => action,
                       "AWSAccessKeyId"   => @aws_access_key_id }
      service_hash.update(param)
      service_params = signed_service_params(@aws_secret_access_key, service_hash, :get, @params[:server], '/')
      request        = Net::HTTP::Get.new("/?#{service_params}")
        # prepare output hash
      { :request  => request, 
        :server   => @params[:server],
        :port     => @params[:port],
        :protocol => @params[:protocol] }
    end

    # TODO port this over to sns from sqs
    # def generate_post_request(action, param={})  # :nodoc:
    # end

    # Sends request to Amazon and parses the response
    # Raises AwsError if any banana happened
    def request_info(request, parser) # :nodoc:
      request_info_impl(:sns_connection, @@bench, request, parser)
    end
    
    def add_permission
      raise "todo"
    end
    
    def confirm_subscription(topic_arn, token, auth_on_unsubscribe = false)
      req_hash = generate_request('ConfirmSubscription', 'TopicArn' => topic_arn,
                                  'Token' => token, 
                                  'AuthenticateOnUnsubscribe' => auth_on_unsubscribe.to_s)
      request_info(req_hash, SnsSubscribeParser.new(:logger => @logger))                            
    rescue
      on_exception
    end
  
    # Creates a new queue, returning its URI.
    #
    #  sns.create_topic('my_topic') #=> TopicArn string
    #
    def create_topic(topic_name)
      req_hash = generate_request('CreateTopic', 'Name' => topic_name)
      request_info(req_hash, SnsCreateTopicParser.new(:logger => @logger))
    rescue
      on_exception
    end
    
    # Delete the topic
    #
    # sns.delete_topic("arn:aws:sns:us-east-1:734188402028:My-Topic")
    def delete_topic(topic_arn)
      req_hash = generate_request('DeleteTopic', 'TopicArn' => topic_arn)
      request_info(req_hash, SnsStatusParser.new(:logger => @logger))
    rescue
      on_exception
    end
    
    def get_topic_attributes(topic_arn)
      req_hash = generate_request('GetTopicAttributes', 'TopicArn' => topic_arn)
      request_info(req_hash, SnsGetTopicAttributesParser.new(:logger => @logger))
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
    
    def list_topics(next_token = nil)
      req_hash = generate_request('ListTopics', 'NextToken' => next_token)
      request_info(req_hash, SnsListTopicsParser.new(:logger => @logger))
    rescue
      on_exception
    end
    
    # TODO deal with utf constraints on message & subject
    def publish(topic_arn, message, subject = nil)
      req_hash = generate_request('Publish', 'TopicArn' => topic_arn, 'Message' => message, 'Subject' => subject)
      request_info(req_hash, SnsPublishParser.new(:logger => @logger))
    rescue
      on_exception
    end
    
    def remove_permission
      raise "todo"
    end

    # Set an attribute for the topic.
    # Note that Amazon called the api SetTopicAttributes (plural) but
    # it's really singular in the current API.
    def set_topic_attributes(topic_arn, attr_name, attr_value)
      req_hash = generate_request('SetTopicAttributes', 'TopicArn' => topic_arn,
                                  'AttributeName' => attr_name, 'AttributeValue' => attr_value)
      request_info(req_hash, SnsStatusParser.new(:logger => @logger))
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
  
  # Parsers

  class SnsCreateTopicParser < RightAWSParser # :nodoc:
    def tagend(name)
      @result = @text if name == 'TopicArn'
    end
  end

  class SnsStatusParser < RightAWSParser # :nodoc:
    def tagend(name)
      if name == 'ResponseMetadata'
        @result = true
      end
    end
  end

  class SnsPublishParser < RightAWSParser # :nodoc:
    def tagend(name)
      @result = @text if name == 'MessageId'
    end
  end

  class SnsGetTopicAttributesParser < RightAWSParser # :nodoc:
    def reset
      @result = {}
    end

    def tagend(name)
      case name 
        when 'key' then @current_attribute = @text
        when 'value' then @result[@current_attribute] = @text
      end
    end
  end

  # Subscription parsers

  class SnsSubscribeParser < RightAWSParser # :nodoc:
    def tagend(name)
      @result = @text if name == 'SubscriptionArn'
    end
  end

  #TODO get the next token
  class SnsListSubscriptionsParser < RightAWSParser # :nodoc:
    def reset
      @result = []
    end

    def tagstart(name, attributes)
      @current_member = {} if name == 'Subscriptions'
    end

    def tagend(name)
      case name
      when 'TopicArn'     then @current_member['TopicArn'] = @text
      when 'Protocol'     then @current_member['Protocol'] = @text
      when 'SubscriptionArn' then @current_member['SubscriptionArn'] = @text
      when 'Owner'        then @current_member['Owner'] = @text
      when 'Endpoint'     then @current_member['Endpoint'] = @text; @result << @current_member
      end
    end
  end

  # Topic parsers
  class SnsListTopicsParser < RightAWSParser # :nodoc:
    def reset
      @result = []
    end

    def tagend(name)
      @result << @text if name == "TopicArn"
    end
  end
end