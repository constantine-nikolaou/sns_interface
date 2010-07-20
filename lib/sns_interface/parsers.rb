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