using System;
using Confluent.Kafka;
using System.Collections;
using System.Collections.Generic;
using System.Threading;
using UnityEngine;

public class KafkaConnect : MonoBehaviour
{
    ConsumerConfig conf = new ConsumerConfig
    {
        GroupId = "test-consumer-group",
        BootstrapServers = "localhost:9092",
        // Note: The AutoOffsetReset property determines the start offset in the event
        // there are not yet any committed offsets for the consumer group for the
        // topic/partitions of interest. By default, offsets are committed
        // automatically, so in this example, consumption will only start from the
        // earliest message in the topic 'my-topic' the first time you run the program.
        AutoOffsetReset = AutoOffsetReset.Earliest
    };
    // Start is called before the first frame update
    void Start()
    {
        
    }

    // https://github.com/confluentinc/confluent-kafka-dotnet#basic-consumer-example
    void Connect()
    {
        using (var c = new ConsumerBuilder<Ignore, string>(conf).Build())
        {
            c.Subscribe("my-topic");

            var cts = new CancellationTokenSource();
            // Console.CancelKeyPress += (_, e) => {
            //     e.Cancel = true; // prevent the process from terminating.
            //     cts.Cancel();
            // };

            try
            {
                while (true)
                {
                    var cr = c.Consume(cts.Token);
                    Debug.LogFormat("KafkaConnect.Connect Consumed message: {0}", cr.Message);
                }
            }
            catch (ConsumeException e)
            {
                Debug.LogErrorFormat("KafkaConnect.Connect ConsumeException: {0}", e.Error.Reason);
            }
            catch (OperationCanceledException)
            {
                // Ensure the consumer leaves the group cleanly and final offsets are committed.
                c.Close();
            }
            catch(Exception e)
            {
                Debug.LogErrorFormat("KafkaConnect.Connect: {0}", e);
            }
        }
    }
}
