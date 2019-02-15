package main

import (
	"bytes"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
)

const (
	//servAddr = "140.90.128.146:1000"
	servAddr = "2.pool.iemwin.net:2211"
	mask     = 0xFF
	filepath = "/home/acb/emwin/"
)

var (
	buffer []byte
	header []byte
	data   []byte
	files  map[string][][]byte
	last   = make(map[string]string)
	conn   *net.TCPConn
)

var startFlag = []byte{47, 80, 70} // "/PF"
var q chan []byte

func main() {
	c := make(chan []byte, 20)
	go byteblaster(c)
	for {
		data = <-c
		for _, value := range data {
			buffer = append(buffer, value)
		}
		extractBlocks()
	}
}

// pull out and process all blocks in current buffer
func extractBlocks() {
	// find block start and remove what's before
	for len(buffer) > 1116 {
		blockStart := bytes.Index(buffer, startFlag)
		if blockStart > -1 {
			buffer = buffer[blockStart:]
			blockEnd := bytes.Index(buffer[3:], startFlag)
			if blockEnd > -1 {
				block := buffer[:blockEnd+3]
				buffer = buffer[blockEnd+3:]
				fileIt(block)
			} else {
				break
			}
		}
	}
}

// File ... Assemble received blocks into files and store
func fileIt(block []byte) {
	header = block[0:80]
	filename := string(header[3:15])
	part := cleanint(header[18:20])
	parts := cleanint(header[27:33])
	filedate := strings.TrimRight(string(header[47:]), " \n")
	data = block[80:1104]
	// if the checksum matches, add to file collector
	if checked(header, data) {
		buildfile(filename, filedate, part, parts, data)
	} 
}

func buildfile(filename string, filedate string, part int, parts int, data []byte) {
	// if we already have this version, ignore this block
	if !(filedate > last[filename]) {
		return
	}
	// in case of SAHOURLY.TXT, TAFALLUS.TXT (et al.?) do files need to be concatenated?

	// if we aren't already assembling this one, create an accumulator for it
	if len(files[filename]) == 0 {
		newfile(filename, filedate, part, parts, data)
	} else {
		// if a known filename and the same date, add block to existing file
		if string(files[filename][0]) == filedate {
			files[filename][part] = data
		} else { // but if date is different, create a new file structure for that filename
			newfile(filename, filedate, part, parts, data)
		}
	}
	// if file structure for current filename is complete, save it as file, clear from collector
	if iscomplete(filename) {
		filebytes := make([]byte, 1024)
		i := 0
		for i = 1; i < parts+1; i++ {
			filebytes = append(filebytes, files[filename][i]...)
		}
		if filename != "FILLFILE.TXT" {
			store(filename, filedate, filebytes)
			last[filename] = filedate
		}
		delete(files, filename)
	}
}

// create a new file structure ([][]byte), initialize with header and current block
func newfile(filename string, filedate string, part int, parts int, data []byte) {
	file := make([][]byte, parts+1)
	files = make(map[string][][]byte)
	files[filename] = file
	files[filename][0] = []byte(filedate)
	files[filename][part] = data
}

// test whether stated checksum matches data payload
func checked(header []byte, data []byte) bool {
	cs := cleanint(header[36:43])
	sum := checkSum(data)
	if cs == sum {
		return true
	}
	return false
}

// return a QBT check sum of payload
func checkSum(data []byte) int {
	sum := 0
	for _, value := range data {
		sum = sum + int(value)
	}
	return sum
}

// test whether all blocks of a file have been received
func iscomplete(filename string) bool {
	if _, key := files[filename]; !key {
		return false
	}
	for _, block := range files[filename] {
		if len(block) == 0 {
			return false
		}
	}
	return true
}

// remove whitespace and return int value of string
func cleanint(value []byte) int {
	integer, _ := strconv.Atoi(strings.TrimSpace(string(value)))
	return integer
}

func store(filename string, filedate string, product []byte) {
	l := strconv.Itoa(len(product))
	log.Println("Saving", filename, "("+l+")", filedate)
	filepath := filepath + filename
	f, err := os.Create(filepath)
	if err != nil {
		log.Println("File Open Error", err)
	}
	defer f.Close()
	_, err = f.Write(product)
	if err != nil {
		log.Println("File Write Error", err)
	}
	// if .ZIS, unzip

	f.Sync()
}

// Run ... get data from EMWIN server
func byteblaster(ch chan []byte) {

	conn = connect()

	data := make([]byte, 1116)
	d2 := make([]byte, 1116)
	for {
		_, err := conn.Read(data)
		if err != nil {
			conn = connect()
		}
		for _, octet := range data {
			d2 = append(d2, octet^mask) // EMWIN-required XOR of received bytes
		}
		ch <- d2
	}
}

func connect() *net.TCPConn {
	fmt.Println("Starting ByteBlaster interface at", servAddr)
	tcpAddr, err := net.ResolveTCPAddr("tcp", servAddr)
	if err != nil {
		log.Panic("ResolveTCPAddr failed:", err.Error())
		os.Exit(1)
	}
	conn, err = net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		log.Panic("Dial failed:", err.Error())
		os.Exit(1)
	}
	return conn
}
