# Epson TM-T20III on RS-232

An Epson TM-T20III receipt printer, connected through an ATEN UC-232A USB to
RS-232 adapter (USB id `0557:2008`, a Prolific PL2303). It appears on macOS as
`/dev/cu.usbserial-<n>`. Measured on 2026-10-04 for T14 (PLAN-V2.md), with
`spike/serialbench` (`make spike-serialbench`), which uses go.bug.st/serial
v1.8.0 as the agent does.

## Line settings: 9600 baud, 8N1

Found without printing, by asking for the printer's real-time status (ESC/POS
`DLE EOT n`) at each rate in turn:

```
serialbench status --path /dev/cu.usbserial-111420
baud   9600  DLE EOT 1 (printer status): reply 16  well-formed
baud   9600  DLE EOT 2 (offline status): reply 12  well-formed
baud   9600  DLE EOT 3 (error status): reply 12  well-formed
baud   9600  DLE EOT 4 (paper roll status): reply 12  well-formed
```

A reply is well-formed when bits 1 and 4 are set and bits 0 and 7 are clear,
which every `DLE EOT` reply has. 9600 was the first rate tried. The offline,
error and paper replies, `12`, have no condition bit set: the cover was shut,
nothing had failed, and there was paper.

A 20-line job printed as sent, and the cut worked (`GS V 65 n`).

## The operating system accepts a job long before it reaches the printer

```
serialbench print --path /dev/cu.usbserial-111420 --baud 9600 --lines 200 --wait-feed
job: 8281 bytes, 200 numbered lines, at 9600 baud
write(2) accepted it all after 2ms, in 1 Write calls, the slowest 0s
Drain returned after 2ms; the line needs 8.626s for this many bytes
```

The job left write(2) in one call, 2 ms after it started, and `Drain`
(TIOCDRAIN on macOS) returned at the same moment, although 8281 bytes need
8.6 s at 9600 baud. Between them, the operating system and the adapter held the
whole job. The 20-line job showed the same: status requests sent right after
`Drain` returned were answered only about a second later, once the job ahead of
them in the adapter had gone out.

So neither write(2) nor `Drain` says when the bytes have left the wire with
this adapter on macOS. A tx's `written` result can only say that the operating
system accepted every byte, which is what DESIGN-V2.md, "Writing: tx", decides.

## No flow control signals

During the 200-line job, DSR, CTS, DCD and RI read false throughout, sampled
every 10 ms, and the printer sent no bytes, so no XON or XOFF. After the job
its status was clean (`16 12 12 12`).

The cover was to be opened for about 6 s during that job, so that the printer
would stop while data kept arriving. The operator reported the print complete
and consistent, without saying whether the cover had been opened.

What this does not show: whether the printer is set to DTR/DSR or XON/XOFF
handshaking (its self-test print lists it), whether the adapter's cable carries
the handshake lines, and what a job larger than the printer's receive buffer
does while the printer is stopped. go.bug.st/serial cannot turn flow control on
in any case (#5).
