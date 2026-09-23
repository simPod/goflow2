package kafka

import (
	"fmt"
	"testing"
	"time"

	sarama "github.com/Shopify/sarama"
)

func TestPoolMockBrokerIntegration(t *testing.T) {
	r := poolRegistry(t)
	broker := sarama.NewMockBroker(t, 0)
	defer broker.Close()
	d := poolDriver(t, 2, true)
	d.kafkaBrk = broker.Addr()
	d.kafkaDiagnosticSuccesses = true
	d.kafkaFlushFrequency = 10 * time.Millisecond
	broker.SetHandlerByMap(map[string]sarama.MockResponse{
		"ApiVersionsRequest": sarama.NewMockApiVersionsResponse(t),
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetBroker(broker.Addr(), broker.BrokerID()).
			SetController(broker.BrokerID()).
			SetLeader(d.kafkaTopic, 0, broker.BrokerID()),
		"ProduceRequest": sarama.NewMockProduceResponse(t).SetVersion(3).
			SetError(d.kafkaTopic, 0, sarama.ErrNoError),
	})
	// Exercise the public initializer, with two real NewAsyncProducer clients.
	if err := d.Init(); err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	for i := 0; i < 20; i++ {
		if err := d.Send(nil, []byte(fmt.Sprint(i))); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		select {
		case err := <-d.Errors():
			t.Fatal(err)
		default:
		}
		f := gatherDiagnostics(t, r)
		total := float64(0)
		for i := 0; i < 2; i++ {
			total += metricValue(t, f, "goflow2_kafka_producer_successes_total", map[string]string{"producer": fmt.Sprint(i)})
		}
		if total == 20 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %v successful completions", total)
		}
		time.Sleep(10 * time.Millisecond)
	}
	f := gatherDiagnostics(t, r)
	if metricValue(t, f, "goflow2_kafka_producers", nil) != 2 {
		t.Fatal("pool size")
	}
	for i := 0; i < 2; i++ {
		labels := map[string]string{"producer": fmt.Sprint(i)}
		if metricValue(t, f, "goflow2_kafka_producer_successes_total", labels) != 10 {
			t.Fatal("unbalanced completions")
		}
		labels["broker"] = "0"
		if metricValue(t, f, "goflow2_kafka_requests_total", labels) < 1 {
			t.Fatal("no broker requests")
		}
		labels["broker"] = "all"
		if metricValue(t, f, "goflow2_kafka_records_sent_total", labels) != 10 {
			t.Fatal("wrong record count")
		}
	}
	metadata, produce := 0, 0
	for _, exchange := range broker.History() {
		switch exchange.Request.(type) {
		case *sarama.MetadataRequest:
			metadata++
		case *sarama.ProduceRequest:
			produce++
		}
	}
	if metadata < 2 || produce < 2 {
		t.Fatalf("metadata=%d produce=%d", metadata, produce)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	if len(gatherDiagnostics(t, r)) != 0 {
		t.Fatal("metrics leaked")
	}
	if len(d.errors) != 0 {
		t.Fatal("unexpected producer errors")
	}
}

func TestPoolMockBrokerCloseFlush(t *testing.T) {
	r, snapshots := snapshotPoolRegistry(t)
	broker := sarama.NewMockBroker(t, 0)
	defer broker.Close()
	d := poolDriver(t, 2, true)
	d.kafkaBrk = broker.Addr()
	d.kafkaDiagnosticSuccesses = true
	// Leave batches pending until the normal five-second timer. Sarama 1.38.1
	// AsyncClose waits for this timer rather than forcing an immediate flush.
	d.kafkaFlushFrequency = 5 * time.Second
	d.kafkaFlushBytes = 1 << 20
	broker.SetHandlerByMap(map[string]sarama.MockResponse{
		"ApiVersionsRequest": sarama.NewMockApiVersionsResponse(t),
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetBroker(broker.Addr(), broker.BrokerID()).
			SetController(broker.BrokerID()).
			SetLeader(d.kafkaTopic, 0, broker.BrokerID()),
		"ProduceRequest": sarama.NewMockProduceResponse(t).SetVersion(3).
			SetError(d.kafkaTopic, 0, sarama.ErrNoError),
	})
	if err := d.Init(); err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	for i := 0; i < 20; i++ {
		if err := d.Send(nil, []byte(fmt.Sprintf("pending-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	// Deliberately do not wait for acknowledgements or poll completion metrics.
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	if snapshots.err != nil {
		t.Fatal(snapshots.err)
	}
	// Sarama 1.38.1 keeps ProduceRequest.records private. Successful completions
	// with WaitForLocal prove all 20 messages received a broker acknowledgement.
	for i := 0; i < 2; i++ {
		labels := map[string]string{"producer": fmt.Sprint(i)}
		if got := metricValue(t, snapshots.families, "goflow2_kafka_producer_successes_total", labels); got != 10 {
			t.Fatalf("producer %d: close completed with %v acknowledgements, want 10", i, got)
		}
	}
	metadata, produce := 0, 0
	for _, exchange := range broker.History() {
		switch request := exchange.Request.(type) {
		case *sarama.MetadataRequest:
			metadata++
		case *sarama.ProduceRequest:
			produce++
			if request.RequiredAcks != sarama.WaitForLocal {
				t.Fatalf("unexpected acknowledgement mode: %v", request.RequiredAcks)
			}
		}
	}
	if metadata < 2 || produce < 2 {
		t.Fatalf("metadata=%d produce=%d", metadata, produce)
	}
	if len(gatherDiagnostics(t, r)) != 0 {
		t.Fatal("metrics leaked")
	}
	if len(d.errors) != 0 {
		t.Fatal("unexpected producer errors")
	}
}
