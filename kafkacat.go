package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"gopkg.in/alecthomas/kingpin.v2"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

const (
	SpringHeaderKey          = "spring_json_header_types"
	SpringHeaderDefaultValue = "java.lang.String"
	DoubleQuote              = string('"')
)

var (
	verbosity    = 1
	exitEOF      = false
	eofCnt       = 0
	partitionCnt = 0
	keyDelim     = ""
	messageDelim = ""
	sigs         chan os.Signal
)

func runProducer(config *kafka.ConfigMap, topic string, headers map[string]string, partition int32, springMetadata map[string]string) {
	p, err := kafka.NewProducer(config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create producer: %s\n", err)
		os.Exit(1)
	}

	fmt.Fprintf(os.Stderr, "Created Producer %v, topic %s [%d]\n", p, topic, partition)

	tp := kafka.TopicPartition{Topic: &topic, Partition: partition}

	go func(drs chan kafka.Event) {
		for ev := range drs {
			m, ok := ev.(*kafka.Message)
			if !ok {
				continue
			}
			if m.TopicPartition.Error != nil {
				fmt.Fprintf(os.Stderr, "%% Delivery error: %v\n", m.TopicPartition)
			} else if verbosity >= 2 {
				fmt.Fprintf(os.Stderr, "%% Delivered %v\n", m)
			}
		}
	}(p.Events())

	reader := bufio.NewReader(os.Stdin)
	stdinChan := make(chan string)

	go func() {
		for true {
			line, err := reader.ReadString('\n')
			if err != nil {
				break
			}

			line = strings.TrimSuffix(line, "\n")
			if len(line) == 0 {
				continue
			}

			stdinChan <- line
		}
		close(stdinChan)
	}()

	run := true

	for run == true {
		select {
		case sig := <-sigs:
			fmt.Fprintf(os.Stderr, "%% Terminating on signal %v\n", sig)
			run = false

		case line, ok := <-stdinChan:
			if !ok {
				run = false
				break
			}

			msg := kafka.Message{TopicPartition: tp}

			if keyDelim != "" {
				vec := strings.SplitN(line, keyDelim, 2)
				if len(vec[0]) > 0 {
					msg.Key = ([]byte)(vec[0])
				}
				if len(vec) == 2 && len(vec[1]) > 0 {
					msg.Value = ([]byte)(vec[1])
				}
			} else {
				if len(headers) != 0 {
					kafkaHeaders := make([]kafka.Header, 0)
					springHeaders := make(map[string]string)
					for k, v := range headers {
						if len(springMetadata) != 0 {
							if _, ok := springMetadata[k]; ok {
								springHeaders[k] = springMetadata[k]
							} else {
								springHeaders[k] = SpringHeaderDefaultValue
							}
						}
						if value, ok := springHeaders[k]; ok && value == SpringHeaderDefaultValue {
							// костыль для решения проблемы парсинга enum
							kafkaHeaders = append(kafkaHeaders, kafka.Header{Key: k, Value: ([]byte)(DoubleQuote + v + DoubleQuote)})
						} else {
							kafkaHeaders = append(kafkaHeaders, kafka.Header{Key: k, Value: ([]byte)(v)})
						}
					}
					if len(springHeaders) != 0 {
						if jsonString, err := json.Marshal(springHeaders); err != nil {
							return
						} else {
							kafkaHeaders = append(kafkaHeaders, kafka.Header{Key: SpringHeaderKey, Value: jsonString})
						}
					}
					msg.Headers = kafkaHeaders
				}
				msg.Value = ([]byte)(line)
			}

			p.ProduceChannel() <- &msg
		}
	}

	fmt.Fprintf(os.Stderr, "%% Flushing %d message(s)\n", p.Len())
	p.Flush(10000)
	fmt.Fprintf(os.Stderr, "%% Closing\n")
	p.Close()
}

func runConsumer(config *kafka.ConfigMap, topics []string, header bool, messageDelim string) {
	c, err := kafka.NewConsumer(config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
		os.Exit(1)
	}

	fmt.Fprintf(os.Stderr, "%% Created Consumer %v\n", c)

	c.SubscribeTopics(topics, nil)

	run := true

	for run == true {
		select {

		case sig := <-sigs:
			fmt.Fprintf(os.Stderr, "%% Terminating on signal %v\n", sig)
			run = false

		case ev := <-c.Events():
			switch e := ev.(type) {
			case kafka.AssignedPartitions:
				fmt.Fprintf(os.Stderr, "%% %v\n", e)
				c.Assign(e.Partitions)
				partitionCnt = len(e.Partitions)
				eofCnt = 0
			case kafka.RevokedPartitions:
				fmt.Fprintf(os.Stderr, "%% %v\n", e)
				c.Unassign()
				partitionCnt = 0
				eofCnt = 0
			case *kafka.Message:
				fmt.Printf("timestamp: %s\n", e.Timestamp)
				if verbosity >= 2 {
					fmt.Fprintf(os.Stderr, "offset: %s\n", e.TopicPartition)
				}
				if header {
					fmt.Printf("headers: %s\n", e.Headers)
				}
				if keyDelim != "" {
					if e.Key != nil {
						fmt.Printf("%s%s", string(e.Key), keyDelim)
					} else {
						fmt.Printf("%s", keyDelim)
					}
				}
				fmt.Printf("payload: %s\n%s\n", strings.TrimSuffix(string(e.Value), "\n"), messageDelim)
			case kafka.PartitionEOF:
				fmt.Fprintf(os.Stderr, "%% Reached %v\n", e)
				eofCnt++
				if exitEOF && eofCnt >= partitionCnt {
					run = false
				}
			case kafka.Error:
				fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
				run = false
			case kafka.OffsetsCommitted:
				if verbosity >= 2 {
					fmt.Fprintf(os.Stderr, "%% %v\n", e)
				}
			default:
				fmt.Fprintf(os.Stderr, "%% Unhandled event %T ignored: %v\n", e, e)
			}
		}
	}

	fmt.Fprintf(os.Stderr, "%% Closing consumer\n")
	c.Close()
}

type configArgs struct {
	conf kafka.ConfigMap
}

//TODO: add String() method
func (c *configArgs) String() string {
	return ""
}

func (c *configArgs) Set(value string) error {
	return c.conf.Set(value)
}

func (c *configArgs) IsCumulative() bool {
	return true
}

func main() {
	sigs = make(chan os.Signal)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	_, libver := kafka.LibraryVersion()
	kingpin.Version(fmt.Sprintf("confluent-kafka-go (librdkafka v%s)", libver))

	// Default config
	var confargs configArgs
	confargs.conf = kafka.ConfigMap{"session.timeout.ms": 6000}

	/* General options */
	brokers := kingpin.Flag("brokers", "Bootstrap broker(s)").Short('b').Required().String()
	kingpin.Flag("config", "Configuration property (prop=val)").Short('X').PlaceHolder("PROP=VAL").SetValue(&confargs)
	keyDelimArg := kingpin.Flag("key-delim", "Key and value delimiter (empty string=dont print/parse key)").Default("").String()
	verbosityArg := kingpin.Flag("verbosity", "Output verbosity level").Short('v').Default("1").Int()

	/* Producer mode options */
	modeP := kingpin.Command("produce", "Produce messages")
	topic := modeP.Flag("topic", "Topic to produce to").Short('t').Required().String()
	headers := modeP.Flag("headers", "Produce messages with headers").Short('h').PlaceHolder("HEADER=VALUE").StringMap()
	partition := modeP.Flag("partition", "Partition to produce to").Short('p').Default("-1").Int()
	springMetadata := modeP.Flag("spring_metadata", "Add Spring metadata headers").Short('s').PlaceHolder("HEADER=CLASS").StringMap()

	/* Consumer mode options */
	modeC := kingpin.Command("consume", "Consume messages").Default()
	group := modeC.Flag("group", "Consumer group").Short('g').Required().String()
	topics := modeC.Flag("topic", "Topic(s) to subscribe to").Short('t').Required().Strings()
	header := modeC.Flag("headers", "Print headers").Short('h').Bool()
	messageDelimArg := modeC.Flag("message-delim", "Message delimiter").Default("---").String()
	initialOffset := modeC.Flag("offset", "Initial offset").Short('o').Default(kafka.OffsetBeginning.String()).String()
	exitEOFArg := modeC.Flag("eof", "Exit when EOF is reached for all partitions").Short('e').Bool()

	mode := kingpin.Parse()

	verbosity = *verbosityArg
	keyDelim = *keyDelimArg
	exitEOF = *exitEOFArg
	confargs.conf["bootstrap.servers"] = *brokers

	switch mode {
	case "produce":
		confargs.conf["default.topic.config"] = kafka.ConfigMap{"produce.offset.report": true}
		runProducer((*kafka.ConfigMap)(&confargs.conf), *topic, *headers, int32(*partition), *springMetadata)

	case "consume":
		confargs.conf["group.id"] = *group
		confargs.conf["go.events.channel.enable"] = true
		confargs.conf["go.application.rebalance.enable"] = true
		confargs.conf["default.topic.config"] = kafka.ConfigMap{"auto.offset.reset": *initialOffset}
		messageDelim = *messageDelimArg
		runConsumer((*kafka.ConfigMap)(&confargs.conf), *topics, *header, messageDelim)
	}

}
