/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package golang

import (
	"context"
	"fmt"
	"sync"
	"time"

	v2 "github.com/apache/rocketmq-clients/golang/protocol/v2"
	"google.golang.org/protobuf/types/known/durationpb"
)

type PushConsumer interface {
	Consumer

	Start() error
	GracefulStop() error
	Subscribe(topic string, filterExpression *FilterExpression) error
	Unsubscribe(topic string) error
}

var _ = PushConsumer(&defaultPushConsumer{})

type defaultPushConsumer struct {
	groupName string

	cli                         *defaultClient
	subscriptionExpressionsLock sync.RWMutex
	subscriptionExpressions     map[string]*FilterExpression
}

var NewPushConsumer = func(config *Config, opts ...PushConsumerOption) (PushConsumer, error) {
	c, err := NewClient(config)
	if err != nil {
		return nil, err
	}
	unifiedOpt := &pushConsumerOptions{}
	for _, opt := range opts {
		opt(unifiedOpt)
	}
	pc := &defaultPushConsumer{
		cli:                     c.(*defaultClient),
		groupName:               config.ConsumerGroup,
		subscriptionExpressions: unifiedOpt.subscriptionExpressions,
	}
	pc.cli.clientImpl = pc
	return pc, nil
}

func (pc *defaultPushConsumer) GetGroupName() string {
	return pc.groupName
}

func (pc *defaultPushConsumer) wrapReceiveMessageRequest(batchSize int, messageQueue *v2.MessageQueue, filterExpression *FilterExpression, invisibleDuration time.Duration) *v2.ReceiveMessageRequest {
	return &v2.ReceiveMessageRequest{
		Group: &v2.Resource{
			Name: pc.groupName,
		},
		MessageQueue: messageQueue,
		FilterExpression: &v2.FilterExpression{
			Expression: filterExpression.expression,
		},
		BatchSize:         int32(batchSize),
		InvisibleDuration: durationpb.New(invisibleDuration),
		AutoRenew:         false,
	}
}

func (pc *defaultPushConsumer) Start() error {
	err := pc.cli.startUp()
	if err == nil {
		return nil
	}
	err2 := pc.GracefulStop()
	if err2 != nil {
		return fmt.Errorf("startUp err=%w, shutdown err=%v", err, err2)
	}
	return err
}

func (pc *defaultPushConsumer) GracefulStop() error {
	return nil
}

//TODO: handle err for getMessageQueues
func (pc *defaultPushConsumer) Subscribe(topic string, filterExpression *FilterExpression) error {
	pc.cli.getMessageQueues(context.Background(), topic)
	pc.subscriptionExpressionsLock.Lock()
	defer pc.subscriptionExpressionsLock.Unlock()

	pc.subscriptionExpressions[topic] = filterExpression

	return nil
}

//TODO: handle err for getMessageQueues
func (pc *defaultPushConsumer) Unsubscribe(topic string) error {
	pc.cli.getMessageQueues(context.Background(), topic)
	pc.subscriptionExpressionsLock.Lock()
	defer pc.subscriptionExpressionsLock.Unlock()

	delete(pc.subscriptionExpressions, topic)

	return nil
}

func (pc *defaultPushConsumer) isClient() {}

func (pc *defaultPushConsumer) wrapHeartbeatRequest() *v2.HeartbeatRequest {
	return &v2.HeartbeatRequest{
		Group: &v2.Resource{
			Name: pc.groupName,
		},
		ClientType: v2.ClientType_PUSH_CONSUMER,
	}
}

func (pc *defaultPushConsumer) onRecoverOrphanedTransactionCommand(endpoints *v2.Endpoints, command *v2.RecoverOrphanedTransactionCommand) error {
	return nil
}

func (pc *defaultPushConsumer) onVerifyMessageCommand(endpoints *v2.Endpoints, command *v2.VerifyMessageCommand) error {
	return nil
}
