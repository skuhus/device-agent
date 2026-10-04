// Command serialbench measures a serial printer on the bench before the agent
// writes to devices (PLAN-V2.md, T14). It is not part of the agent.
//
// It speaks ESC/POS, the command language of Epson's receipt printers, through
// go.bug.st/serial, the library the agent uses, so that what it measures is what
// the agent will meet:
//
//	status  asks for the printer's real-time status (DLE EOT) at each baud rate
//	        given. Only the right line settings get a well-formed reply, so this
//	        finds them without printing.
//	print   prints a job of numbered lines, so that a lost or damaged line can be
//	        seen on the paper, and reports how long write(2) took to accept the
//	        job and how long until Drain said it had left the port.
//
// Neither mode turns flow control on: the library cannot (#5).
package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"go.bug.st/serial"
)

// ESC/POS commands, from Epson's ESC/POS command reference.
var (
	escInit      = []byte{0x1b, 0x40}       // ESC @: initialise
	dleEOT       = []byte{0x10, 0x04}       // DLE EOT n: real-time status, n = 1 to 4
	feedAndCut   = []byte{0x1d, 0x56, 0x41} // GS V 65 n: feed n units past the cutter, then cut
	statusNames  = map[byte]string{1: "printer", 2: "offline", 3: "error", 4: "paper roll"}
	statusFixed  = byte(0x93) // bits 0, 1, 4 and 7 are fixed in every DLE EOT reply
	statusExpect = byte(0x12) // bits 1 and 4 set, bits 0 and 7 clear
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: serialbench status|print [flags]")
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "status":
		err = runStatus(os.Args[2:])
	case "print":
		err = runPrint(os.Args[2:])
	default:
		err = fmt.Errorf("unknown mode %q; expected status or print", os.Args[1])
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error: "+err.Error())
		os.Exit(1)
	}
}

type lineFlags struct {
	path     *string
	dataBits *int
	parity   *string
	stopBits *int
}

func addLineFlags(fs *flag.FlagSet) lineFlags {
	return lineFlags{
		path:     fs.String("path", "", "serial device, such as /dev/cu.usbserial-111420"),
		dataBits: fs.Int("data-bits", 8, "data bits"),
		parity:   fs.String("parity", "none", "none, odd or even"),
		stopBits: fs.Int("stop-bits", 1, "1 or 2"),
	}
}

func (lf lineFlags) mode(baud int) (*serial.Mode, error) {
	if *lf.path == "" {
		return nil, errors.New("--path is required")
	}
	mode := &serial.Mode{BaudRate: baud, DataBits: *lf.dataBits}
	switch *lf.parity {
	case "none":
		mode.Parity = serial.NoParity
	case "odd":
		mode.Parity = serial.OddParity
	case "even":
		mode.Parity = serial.EvenParity
	default:
		return nil, fmt.Errorf("--parity %q: expected none, odd or even", *lf.parity)
	}
	switch *lf.stopBits {
	case 1:
		mode.StopBits = serial.OneStopBit
	case 2:
		mode.StopBits = serial.TwoStopBits
	default:
		return nil, fmt.Errorf("--stop-bits %d: expected 1 or 2", *lf.stopBits)
	}
	return mode, nil
}

func runStatus(args []string) error {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	lf := addLineFlags(fs)
	bauds := fs.String("bauds", "9600,19200,38400,57600,115200", "baud rates to try, in order")
	wait := fs.Duration("wait", 500*time.Millisecond, "how long to wait for each reply")
	if err := fs.Parse(args); err != nil {
		return err
	}
	for _, field := range strings.Split(*bauds, ",") {
		baud, err := strconv.Atoi(strings.TrimSpace(field))
		if err != nil {
			return fmt.Errorf("--bauds %q: %w", field, err)
		}
		mode, err := lf.mode(baud)
		if err != nil {
			return err
		}
		replies, err := queryStatus(*lf.path, mode, *wait)
		if err != nil {
			return err
		}
		wellFormed := 0
		for n := byte(1); n <= 4; n++ {
			reply := replies[n]
			verdict := "no reply"
			if len(reply) > 0 {
				verdict = "malformed"
				if len(reply) == 1 && reply[0]&statusFixed == statusExpect {
					verdict = "well-formed"
					wellFormed++
				}
			}
			fmt.Printf("baud %6d  DLE EOT %d (%s status): reply % x  %s\n", baud, n, statusNames[n], reply, verdict)
		}
		if wellFormed == 4 {
			fmt.Printf("baud %d: every reply well-formed; these are the printer's line settings\n", baud)
			return nil
		}
	}
	return errors.New("no baud rate tried got four well-formed replies")
}

// queryStatus sends each DLE EOT request in turn and collects what comes back
// within wait. The input buffer is cleared first, so a reply is not confused
// with bytes left over from an earlier rate.
func queryStatus(path string, mode *serial.Mode, wait time.Duration) (map[byte][]byte, error) {
	port, err := serial.Open(path, mode)
	if err != nil {
		return nil, fmt.Errorf("open %s at %d baud: %w", path, mode.BaudRate, err)
	}
	defer port.Close()
	if err := port.SetReadTimeout(50 * time.Millisecond); err != nil {
		return nil, err
	}
	if err := port.ResetInputBuffer(); err != nil {
		return nil, err
	}
	replies := map[byte][]byte{}
	for n := byte(1); n <= 4; n++ {
		if err := writeAll(port, append(bytes.Clone(dleEOT), n)); err != nil {
			return nil, err
		}
		replies[n] = readFor(port, wait)
	}
	return replies, nil
}

func readFor(port serial.Port, wait time.Duration) []byte {
	var got []byte
	buf := make([]byte, 64)
	deadline := time.Now().Add(wait)
	for time.Now().Before(deadline) {
		n, err := port.Read(buf)
		if err != nil {
			break
		}
		got = append(got, buf[:n]...)
	}
	return got
}

// writeAll loops until every byte is written, as the agent will (DESIGN-V2.md,
// "Writing: tx"): the library makes one write(2) call per Write.
func writeAll(port serial.Port, data []byte) error {
	for len(data) > 0 {
		n, err := port.Write(data)
		if err != nil {
			return err
		}
		data = data[n:]
	}
	return nil
}

func runPrint(args []string) error {
	fs := flag.NewFlagSet("print", flag.ContinueOnError)
	lf := addLineFlags(fs)
	baud := fs.Int("baud", 0, "baud rate, as status found it")
	lines := fs.Int("lines", 20, "numbered lines to print")
	chunk := fs.Int("chunk", 0, "bytes per Write call; 0 writes the whole job in one call")
	cut := fs.Bool("cut", true, "feed and cut at the end")
	waitFeed := fs.Bool("wait-feed", false, "start when the printer's FEED button has been pressed and released")
	watch := fs.Duration("watch", 0, "how long to keep watching after the job is written; 0 is the line time plus 15 s")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *baud <= 0 {
		return errors.New("--baud is required")
	}
	mode, err := lf.mode(*baud)
	if err != nil {
		return err
	}
	job := buildJob(*baud, *lines, *cut)

	port, err := serial.Open(*lf.path, mode)
	if err != nil {
		return fmt.Errorf("open %s: %w", *lf.path, err)
	}
	defer port.Close()

	if err := port.SetReadTimeout(20 * time.Millisecond); err != nil {
		return err
	}
	if *waitFeed {
		fmt.Println("waiting for FEED to be pressed and released")
		if err := awaitFeed(port); err != nil {
			return err
		}
	}

	// 8N1 carries 10 bits per byte; other formats a little more or less.
	onLine := time.Duration(float64(len(job)) * 10 / float64(*baud) * float64(time.Second))
	if *watch <= 0 {
		*watch = onLine + 15*time.Second
	}

	size := *chunk
	if size <= 0 {
		size = len(job)
	}
	start := time.Now()
	fmt.Printf("printing %d bytes at %s\n", len(job), start.Format("15:04:05.000"))
	// The handshake lines and whatever the printer sends are recorded
	// throughout. Flow control is off, so the port acts on neither, but a
	// printer that signals busy, on a line or with XON/XOFF, shows here.
	stopSampling := make(chan struct{})
	sampled := make(chan []string, 1)
	received := make(chan []string, 1)
	go func() { sampled <- sampleModemLines(port, start, stopSampling) }()
	go func() { received <- readEverything(port, start, stopSampling) }()
	var slowest time.Duration
	calls := 0
	for rest := job; len(rest) > 0; {
		part := rest[:min(size, len(rest))]
		before := time.Now()
		n, err := port.Write(part)
		calls++
		slowest = max(slowest, time.Since(before))
		if err != nil {
			return fmt.Errorf("write after %d of %d bytes: %w", len(job)-len(rest), len(job), err)
		}
		rest = rest[n:]
	}
	written := time.Since(start)
	if err := port.Drain(); err != nil {
		return fmt.Errorf("drain: %w", err)
	}
	drained := time.Since(start)
	fmt.Printf("written and drained; watching for %s more\n", watch.Round(time.Second))
	time.Sleep(*watch)
	close(stopSampling)
	changes := <-sampled
	incoming := <-received

	fmt.Printf("job: %d bytes, %d numbered lines, at %d baud\n", len(job), *lines, *baud)
	fmt.Printf("write(2) accepted it all after %s, in %d Write calls, the slowest %s\n", written.Round(time.Millisecond), calls, slowest.Round(time.Millisecond))
	fmt.Printf("Drain returned after %s; the line needs %s for this many bytes\n", drained.Round(time.Millisecond), onLine.Round(time.Millisecond))
	fmt.Printf("handshake lines, sampled every 10 ms (%d changes):\n", len(changes)-1)
	for _, change := range changes {
		fmt.Println("  " + change)
	}
	fmt.Printf("bytes the printer sent during the job (%d reads):\n", len(incoming))
	for _, line := range incoming {
		fmt.Println("  " + line)
	}

	for n := byte(1); n <= 4; n++ {
		if err := writeAll(port, append(bytes.Clone(dleEOT), n)); err != nil {
			return err
		}
		fmt.Printf("after the job, DLE EOT %d (%s status): % x\n", n, statusNames[n], readFor(port, 300*time.Millisecond))
	}
	return nil
}

// awaitFeed polls the offline status (DLE EOT 2) until bit 3, paper being fed
// by the FEED button, has been set and has cleared again.
func awaitFeed(port serial.Port) error {
	pressed := false
	for {
		if err := writeAll(port, append(bytes.Clone(dleEOT), 2)); err != nil {
			return err
		}
		reply := readFor(port, 100*time.Millisecond)
		if len(reply) == 1 && reply[0]&statusFixed == statusExpect {
			feeding := reply[0]&0x08 != 0
			if feeding {
				pressed = true
			} else if pressed {
				return nil
			}
		}
	}
}

// readEverything records what the printer sends, with when, until stop is
// closed. XON (0x11) and XOFF (0x13) are named, since they are the printer's
// software flow control.
func readEverything(port serial.Port, start time.Time, stop <-chan struct{}) []string {
	var lines []string
	buf := make([]byte, 256)
	for {
		select {
		case <-stop:
			return lines
		default:
		}
		n, err := port.Read(buf)
		if err != nil {
			return append(lines, "read failed: "+err.Error())
		}
		if n == 0 {
			continue
		}
		var names []string
		for _, b := range buf[:n] {
			switch b {
			case 0x11:
				names = append(names, "XON")
			case 0x13:
				names = append(names, "XOFF")
			}
		}
		lines = append(lines, fmt.Sprintf("%8s  % x  %s", time.Since(start).Round(time.Millisecond), buf[:n], strings.Join(names, " ")))
	}
}

// sampleModemLines records the input modem lines at start and at every change,
// until stop is closed.
func sampleModemLines(port serial.Port, start time.Time, stop <-chan struct{}) []string {
	var changes []string
	var last string
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		bits, err := port.GetModemStatusBits()
		now := "unavailable: " + fmt.Sprint(err)
		if err == nil {
			now = fmt.Sprintf("cts=%t dsr=%t dcd=%t ri=%t", bits.CTS, bits.DSR, bits.DCD, bits.RI)
		}
		if now != last {
			changes = append(changes, fmt.Sprintf("%8s  %s", time.Since(start).Round(time.Millisecond), now))
			last = now
		}
		select {
		case <-stop:
			return changes
		case <-ticker.C:
		}
	}
}

// buildJob is a header, numbered lines whose text is the same every time, so
// that a lost, doubled or damaged line shows on the paper, and an end line.
func buildJob(baud, lines int, cut bool) []byte {
	var job bytes.Buffer
	job.Write(escInit)
	fmt.Fprintf(&job, "serialbench %s\nbaud %d, %d lines\n", time.Now().Format("2006-01-02 15:04:05"), baud, lines)
	for i := 1; i <= lines; i++ {
		fmt.Fprintf(&job, "%04d/%04d ABCDEFGHIJKLMNOPQRSTUVWXYZ0123\n", i, lines)
	}
	fmt.Fprintf(&job, "end of job, %d lines\n", lines)
	if cut {
		job.Write(append(bytes.Clone(feedAndCut), 0x30))
	}
	return job.Bytes()
}
