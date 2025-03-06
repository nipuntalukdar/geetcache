package main

import (
	"bufio"
	"bytes"
	"os"
	"testing"
)

func TestUint32Encoding(t *testing.T) {
	t.Log("Testing uint32 encoding")
	vals := []uint32{4294967295, 1, 0, 10, 100, 300, 3888, 7888888, 88888, 21230000}
	for _, input := range vals {
		var buf bytes.Buffer
		err := writeUint32(input, &buf)
		if err != nil {
			b := buf.Bytes()
			t.Logf("%v ,, len %d\n", b, len(b))
			t.Fatal("Encoding uint32")
		}
		val, err := readUint32(&buf)
		if err != nil {
			t.Fatal("Decoding uint32")
		}
		if val != input {
			t.Fatalf("Incorrect decoded value %d", val)
		}
		t.Logf("Test passed for value:%d\n", input)
	}
}

func TestEncodeToFile(t *testing.T) {
	t.Log("Testing encoding to file")
	file, err := os.Create("ser.bin")
	if err != nil {
		t.Fatal("Could not create file")
	}
	defer os.Remove("ser.bin")
	writer := bufio.NewWriter(file)
	var i uint32 = 0
	for i < 1000000 {
		writeUint32(i, writer)
		i++
	}
	writer.Flush()
	file.Close()
	filein, err := os.Open("ser.bin")
	if err != nil {
		t.Fatal("Could not create file")
	}
	reader := bufio.NewReader(filein)
	i = 0
	for i < 1000000 {
		read, err := readUint32(reader)
		if err != nil {
			t.Fatalf("Error %s", err)
		}
		if i != read {
			t.Fatalf("Incorrect read %d, expected %d", read, i)
		}
		if i&1023 == 1023 {
			t.Logf("%d %d\n", i, read)
		}
		i++
	}
	filein.Close()
}

func TestUint16Encoding(t *testing.T) {
	t.Log("Testing uint16 encoding")
	vals := []uint16{65534, 1, 0, 10, 100, 300, 3888, 10000, 79, 8000}
	for _, input := range vals {
		var buf bytes.Buffer
		err := writeUint16(input, &buf)
		if err != nil {
			b := buf.Bytes()
			t.Logf("%v ,, len %d\n", b, len(b))
			t.Fatal("Encoding uint16")
		}
		val, err := readUint16(&buf)
		if err != nil {
			t.Fatal("Decoding uint16")
		}
		if val != input {
			t.Fatalf("Incorrect decoded value %d", val)
		}
		t.Logf("Test passed for value:%d\n", input)
	}
}

func TestInt64Encoding(t *testing.T) {
	t.Log("Testing int64 encoding")
	vals := []int64{65534, 1, 0, 10, 100, 300, 3888, 10000, 79, 8000}
	for _, input := range vals {
		var buf bytes.Buffer
		err := writeInt64(input, &buf)
		if err != nil {
			b := buf.Bytes()
			t.Logf("%v ,, len %d\n", b, len(b))
			t.Fatal("Encoding int64")
		}
		val, err := readInt64(&buf)
		if err != nil {
			t.Fatal("Decoding int64")
		}
		if val != input {
			t.Fatalf("Incorrect decoded value %d, buflen:%d", val, buf.Len())
		}
		t.Logf("Test passed for value:%d", input)
	}
}

func TestWriteString(t *testing.T) {
	x := "hello world, how are you"
	var buffer bytes.Buffer
	err := writeString(x, &buffer)
	if err != nil {
		t.Errorf("Failed to write string %s\n", x)
	}
	reads, err := readString(&buffer)
	if err != nil {
		t.Errorf("Failed to read string %s\n", x)
	}
	if reads != x {
		t.Errorf("Incorrect string read")
	} else {
		t.Logf("String read is %s\n", reads)
	}
	t.Logf("String encode passed")
}
