# IK2215

## Compile
From the project directory, run:

```bash
javac *.java
```

## Test

```bash
java VSRecv 9000
java VSSend 127.0.0.1:9000 test.txt
tshark -i any -f "udp port 9000" -w capture.pcap
```