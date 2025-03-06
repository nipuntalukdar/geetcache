package main

import (
	"errors"
	"io"
)

func readSureBytes(inp []byte, rd io.Reader) error {
	i := 0
	to_read := len(inp)
	for {
		n, err := rd.Read(inp[i:])
		if err != nil {
			return err
		}
		i += n
		if i == to_read {
			return nil
		}
	}
}

func writeInt64(num int64, wr io.Writer) error {
	i := 7
	buf := make([]byte, 8)
	for {
		buf[i] = uint8(num)
		if i == 0 {
			break
		}
		num >>= 8
		i--
	}
	_, err := wr.Write(buf)
	return err
}

func readInt64(rd io.Reader) (int64, error) {
	buf := make([]byte, 8)
	err := readSureBytes(buf, rd)
	if err != nil {
		return 0, err
	}
	var retval int64 = 0
	i := 0
	for i < 8 {
		retval |= int64(buf[i])
		if i != 7 {
			retval <<= 8
		}
		i++
	}
	return retval, nil
}

func writeUint32(num uint32, wr io.Writer) error {
	i := 3
	buf := make([]byte, 4)
	for {
		buf[i] = uint8(num)
		if i == 0 {
			break
		}
		num >>= 8
		i--
	}
	_, err := wr.Write(buf)
	return err
}

func readUint32(rd io.Reader) (uint32, error) {
	buf := make([]byte, 4)
	err := readSureBytes(buf, rd)
	if err != nil {
		return 0, err
	}
	var retval uint32 = 0
	i := 0
	for i < 4 {
		retval |= uint32(buf[i])
		if i != 3 {
			retval <<= 8
		}
		i++
	}
	return retval, nil
}

func writeUint16(num uint16, wr io.Writer) error {
	buf := []byte{0, 0}
	buf[1] = uint8(num)
	buf[0] = uint8(num >> 8)
	_, err := wr.Write(buf)
	return err
}

func readUint16(rd io.Reader) (uint16, error) {
	buf := []byte{0, 0}
	err := readSureBytes(buf, rd)
	if err != nil {
		return 0, err
	}
	return uint16(buf[0])<<8 | uint16(buf[1]), nil
}

func writeUint8(num uint8, wr io.Writer) error {
	_, err := wr.Write([]byte{byte(num)})
	return err
}

func readUint8(rd io.Reader) (uint8, error) {
	b := []byte{0}
	err := readSureBytes(b, rd)
	if err != nil {
		return 0, err
	}
	return uint8(b[0]), nil
}

func writeBytes(inp []byte, wr io.Writer) error {
	_, err := wr.Write(inp)
	if err != nil {
		return err
	}
	return nil
}

func readBytes(inp []byte, rd io.Reader) error {
	return readSureBytes(inp, rd)
}

func writeString(inp string, wr io.Writer) error {
	//lenght of the string, followed by the string bytes
	//for us string is always less than 256 bytes
	length := len(inp)
	if length == 0 || length > 255 {
		return errors.New("Invalid string length, must be between 1 and 255")
	}
	err := writeUint8(uint8(length), wr)
	if err != nil {
		return err
	}
	return writeBytes([]byte(inp), wr)
}

func readString(rd io.Reader) (string, error) {
	l, err := readUint8(rd)
	if err != nil {
		return "", err
	}
	inp := make([]byte, l)
	err = readBytes(inp, rd)
	if err != nil {
		return "", err
	}
	return string(inp), nil
}

func writeByteArray(inp []byte, wr io.Writer) error {
	if len(inp) == 0 {
		return errors.New("Invalid length passed")
	}
	err := writeUint32(uint32(len(inp)), wr)
	if err != nil {
		return err
	}
	return writeBytes(inp, wr)
}

func readByteArray(rd io.Reader) ([]byte, error) {
	l, err := readUint32(rd)
	if err != nil {
		return nil, err
	}
	inp := make([]byte, l)
	err = readBytes(inp, rd)
	if err != nil {
		return nil, err
	}
	return inp, nil
}
