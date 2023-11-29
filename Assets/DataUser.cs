using UnityEngine;
using UnityEngine.UI;

public class DataUser : MonoBehaviour
{

    public Text display;
    public KafkaConnect connector;
    private StorageKafka Storage => connector.storageKafka;
    
    public void ShowOnDisplay()
    {
        display.text = Storage.ToString();
    }
}
