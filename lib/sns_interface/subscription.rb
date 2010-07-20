class Subscription < SnsInterface
  
  
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
end