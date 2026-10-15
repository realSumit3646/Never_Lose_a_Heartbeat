"""The mock sensor service (biosignal-mock) and its ground-truth checker.

MayaCardio confirmed the mock is not provided and must be built by the
candidate, so this package implements section 6 of the brief: the TCP
protocol, the 10 s buffer, crc32 over the raw data text, signal and ramp
modes, and the faults the judges inject.
"""
