using System;
using Confluent.Kafka;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using UnityEngine;

public class KafkaConnect : MonoBehaviour
{
    ConsumerConfig config = new ConsumerConfig
    {
        GroupId = "test-consumer-group",
        BootstrapServers = "192.168.1.102:9092",
        // Note: The AutoOffsetReset property determines the start offset in the event
        // there are not yet any committed offsets for the consumer group for the
        // topic/partitions of interest. By default, offsets are committed
        // automatically, so in this example, consumption will only start from the
        // earliest message in the topic 'my-topic' the first time you run the program.
        AutoOffsetReset = AutoOffsetReset.Earliest
    };

    [ContextMenu("DebugPrint Kafka")]
    private void DebugStorageKafka() => Debug.LogWarning(storageKafka.ToString());
    public StorageKafka storageKafka = new StorageKafka()
    {
        topics=new List<string>(){"myTopic"}
    };

    private void OnDisable()
    {
        cts?.Cancel();
    }

    // Start is called before the first frame update
    void Start()
    {
    }
        
    [ContextMenu("Connect Kafka")]
    private void testConnect()=> Connect(config, storageKafka);

    [ContextMenu("Disconnect Kafka")]
    private void cancelConnect() => cts?.Cancel();
    
    // https://github.com/confluentinc/confluent-kafka-dotnet#basic-consumer-example
    private CancellationTokenSource cts;
    async Task Connect(ConsumerConfig conf, StorageKafka storage)
    {
        Debug.LogFormat("KafkaConnect.Connect:{0}", conf, storage);
        storage.Init();
        
        using (var c = new ConsumerBuilder<Ignore, string>(conf).Build())
        {
            foreach (var top in storage.topics)
            {
                c.Subscribe(top); //"myTopic"
                Debug.LogFormat("KafkaConnect.Connect Subscribe:{0}", top);
            }

            cts = new CancellationTokenSource();

            try
            {
                while (true)
                {
                    try
                    {
                        var cr = await Task.Run(() => c.Consume(cts.Token));//var cr = c.Consume(cts.Token);
                        Debug.LogFormat("KafkaConnect.Connect Consumed message: {0} {1} {2} {3}", cr.Topic, cr.Offset, cr.Message.Timestamp.UtcDateTime, cr.Message.Value);
                        storage.Insert(cr.Topic, cr);
                    }
                    catch (ConsumeException e)
                    {
                        Debug.LogErrorFormat("KafkaConnect.Connect ConsumeException: {0}", e.Error.Reason);
                    }
                }
            }
            catch (OperationCanceledException e)
            {
                // Ensure the consumer leaves the group cleanly and final offsets are committed.
                c.Close();
                Debug.LogErrorFormat("KafkaConnect.Connect OperationCanceledException: {0}", e.Message);
            }
            catch(Exception e)
            {
                Debug.LogErrorFormat("KafkaConnect.Connect: {0}", e);
            }
        }
    }
}

[Serializable]
public class StorageKafka
{
    public List<string> topics;
    private Dictionary<string, Queue<ConsumeResult<Ignore, string>>> storages;
    public void Insert(string topic, ConsumeResult<Ignore, string> message)
    {
        if (storages == null || !storages.ContainsKey(topic))
            return;
        
        storages[topic].Enqueue(message);
    }

    public void Init()
    {
        if (topics == null || topics.Count == 0)
            return;
        
        storages = new Dictionary<string, Queue<ConsumeResult<Ignore, string>>>();
        foreach (var topic in topics)
        {
            if (storages.ContainsKey(topic))
                continue;
            storages[topic] = new Queue<ConsumeResult<Ignore, string>>();
        }
    }
    
    private StringBuilder sb = new StringBuilder();
    public override string ToString()
    {
        if (storages == null)
            return "";

        sb.Clear();
        foreach (var kvp in storages)
        {
            sb.AppendFormat("Key:{0} Cnt:{1}\n", kvp.Key, kvp.Value.Count);
            if (kvp.Value.Count == 0)
                continue;
            foreach(var val in kvp.Value.ToArray())
                sb.AppendFormat("Val:{0} ", val.Message.Value);
        }

        return sb.ToString();
    }
}
