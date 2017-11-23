#!/usr/bin/env python
from __future__ import division

import pandas
import scipy.io.wavfile as wav
import sys

for s in sys.argv[1:]:
    print('Checking "%s"...' % s)
    csv = pandas.read_csv(s, encoding='utf-8').sort_values('wav_filesize')
    for wav_filename, wav_filesize, transcript in csv.values:
        fs, audio = wav.read(wav_filename)
        if int(len(audio)/fs*1000/10/2) < len(str(transcript)):
            print("Audio too short for transcript: {}".format(wav_filename))
