class Topic
  # Creates a new queue, returning its URI.
  def initialize(name = nil)
    return false if name.nil?
    req_hash = generate_request('CreateTopic', 'Name' => name)
    request_info(req_hash, SnsCreateTopicParser.new(:logger => @logger))
  rescue
    on_exception
  end
  
  # Delete the topic
  #
  # sns.delete_topic("arn:aws:sns:us-east-1:734188402028:My-Topic")
  def delete(topic_arn = nil)
    return false if topic_arn.nil?
    req_hash = generate_request('DeleteTopic', 'TopicArn' => topic_arn)
    request_info(req_hash, SnsStatusParser.new(:logger => @logger))
  rescue
    on_exception
  end
  
  def set_attributes(topic_arn, attr_name, attr_value)
    req_hash = generate_request('SetTopicAttributes', 'TopicArn' => topic_arn,
                                'AttributeName' => attr_name, 'AttributeValue' => attr_value)
    request_info(req_hash, SnsStatusParser.new(:logger => @logger))
  rescue
    on_exception
  end
  
  def get_attributes(topic_arn)
    req_hash = generate_request('GetTopicAttributes', 'TopicArn' => topic_arn)
    request_info(req_hash, SnsGetTopicAttributesParser.new(:logger => @logger))
  rescue
    on_exception
  end
  
  def list(next_token = nil)
    req_hash = generate_request('ListTopics', 'NextToken' => next_token)
    request_info(req_hash, SnsListTopicsParser.new(:logger => @logger))
  rescue
    on_exception
  end
end