package golang

type pushConsumerOptions struct {
	listener                *MessageListener
	maxCacheMessageCount    *int
	maxCacheSizeInBytes     *int
	consumptionThreadCount  *int
	subscriptionExpressions map[string]*FilterExpression
}

type PushConsumerOption func(opt *pushConsumerOptions)

func WithPushConsumerMessageListener(l *MessageListener) PushConsumerOption {
	return func(opt *pushConsumerOptions) {
		opt.listener = l
	}
}

func WithPushConsumerMaxCacheMessageCount(c int) PushConsumerOption {
	return func(opt *pushConsumerOptions) {
		if c < 0 {
			return
		}
		_c := c
		opt.maxCacheMessageCount = &_c
	}
}

func WithPushConsumerMaxCacheSizeInBytes(s int) PushConsumerOption {
	return func(opt *pushConsumerOptions) {
		if s < 0 {
			return
		}
		_s := s
		opt.maxCacheSizeInBytes = &_s
	}
}

func WithPushConsumerConsumptionThreadCount(c int) PushConsumerOption {
	return func(opt *pushConsumerOptions) {
		if c < 0 {
			return
		}
		_c := c
		opt.consumptionThreadCount = &_c
	}
}

func WithPushConsumerSubscriptionExpressions(exs map[string]*FilterExpression) PushConsumerOption {
	return func(opt *pushConsumerOptions) {
		opt.subscriptionExpressions = exs
	}
}
