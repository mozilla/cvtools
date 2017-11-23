#!/usr/bin/env python
from __future__ import absolute_import, division, print_function
import os
import re
import sys
import csv
import glob
import json
import urllib
from random import shuffle
from shutil import copyfile

class Sample(object):
    def __init__(self, filename, text, up_votes, down_votes, age, gender, accent, duration):
        self.filename = filename
        self.text = text
        self.up_votes = up_votes
        self.down_votes = down_votes
        self.age = age
        self.gender = gender
        self.accent = accent
        self.duration = float(duration)

    def __str__(self):
        return 'Text: %s\nFile: %s' % (self.text, self.filename)

# from https://stackoverflow.com/questions/3173320/text-progress-bar-in-the-console
# Print iterations progress
def _print_progress(iteration, total, prefix = 'Progress', suffix = 'complete', decimals = 1, length = 100, fill = '#'):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print('\r%s |%s| %s%% %s' % (prefix, bar, percent, suffix), end = '\r')
    sys.stdout.flush()
    # Print New Line on Complete
    if iteration == total: 
        print()

replace = { \
    '1st': 'first', \
    '2nd': 'second', \
    '3rd': 'third', \
    '4th': 'fourth', \
    '5th': 'fifth', \
    '6th': 'sixth', \
    '7th': 'seventh', \
    '8th': 'eighth', \
    '9th': 'ninth' \
}

drop_tokens = { \
    'jan', \
    'feb', \
    'mar', \
    'apr', \
    'jun', \
    'jul', \
    'aug', \
    'sep', \
    'oct', \
    'nov', \
    'dev', \
    'jan.', \
    'feb.', \
    'mar.', \
    'apr.', \
    'may.', \
    'jun.', \
    'jul.', \
    'aug.', \
    'sep.', \
    'oct.', \
    'nov.', \
    'dev.', \
    'th', \
    'rd', \
    'st' \
}

drop_chars = '$%&'

accepted_chars = " abcdefghijklmnopqrstuvwxyz'"

fieldnames = ['filename', 'text', 'up_votes', 'down_votes', 'age', 'gender', 'accent', 'duration']
collections = [('-dev', 2.0), ('-test', 2.0), ('-train', 100.0)]

def write_sets(root_dir, kind, samples):
    cols = [('', 100)] if kind == 'invalid' else collections
    for set_name, percent in cols:
        set_samples = samples[:int(len(samples)*percent/100.0)]
        dir_name = 'cv-%s%s' % (kind, set_name)
        os.makedirs(root_dir + '/' + dir_name)
        csv_path = '%s.csv' % dir_name
        print('Writing %d samples to "%s"...' % (len(set_samples), csv_path))
        with open(root_dir + '/' + csv_path, 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            i = 0
            _print_progress(0, len(set_samples))
            for i, sample in enumerate(set_samples):
                filename = '%s/sample-%06d.mp3' % (dir_name, i)
                #if sample.filename == '':
                #    print('From: "%s" To: "%s"' % (sample.filename, filename))
                copyfile(sample.filename, root_dir + '/' + filename)
                writer.writerow({ 'filename':   filename,
                                  'text':       sample.text,
                                  'up_votes':   sample.up_votes,
                                  'down_votes': sample.down_votes,
                                  'age':        sample.age,
                                  'gender':     sample.gender,
                                  'accent':     sample.accent })
                _print_progress(i + 1, len(set_samples))
                i += 1

def usage():
    print('Usage: cvgen.py <samples.csv> <target_dir>')

def main():
    samples = []
    args = sys.argv[1:]
    if not len(args) == 2:
        usage()
    samples_csv = os.path.abspath(args[0])
    root_dir = os.path.abspath(args[1])
    if os.path.exists(root_dir):
        print('Target directory must not exist!')
        return
    print('Reading "%s"...' % samples_csv)
    with open(samples_csv) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            samples.append(Sample(**row))

    filtered_samples = []
    num_too_short = 0
    print('Filtering samples...')
    _print_progress(0, len(samples))
    for i, sample in enumerate(samples):
        _print_progress(i + 1, len(samples))
        original_text = urllib.unquote(sample.text)
        text = original_text
        tokens = filter(None, text.lower().strip().split(' '))
        filtered_tokens = []
        skip = False
        for token in tokens:
            if token in replace:
                token = replace[token]
            elif token in drop_tokens:
                skip = True
        text = ' '.join(tokens)
        if re.search('[^0-9]*[0-9][0-9].*', text):
            skip = True
        filtered_text = ''
        for c in text:
            if c in drop_chars:
                skip = True
            elif c in accepted_chars:
                filtered_text += c
        text = filtered_text
        if int(sample.duration*1000/10/2) <= len(text):
            num_too_short += 1
            skip = True
        if skip:
            #print('Skip: %s' % original_text)
            continue
        sample.text = text
        #print('Accepted: %s' % text)
        filtered_samples.append(sample)

    print('Dropped %d samples that are too short for processing.' % num_too_short)
    print('Dropped %d samples due to transcription issues.' % \
        (len(samples) - len(filtered_samples) - num_too_short))

    samples = filtered_samples
    shuffle(samples)

    valid_samples = []
    other_samples = []
    invalid_samples = []
    for sample in samples:
        if sample.up_votes >= 2 and sample.down_votes < sample.up_votes:
            valid_samples.append(sample)
        elif sample.down_votes >= 2 and sample.up_votes < sample.down_votes:
            invalid_samples.append(sample)
        else:
            other_samples.append(sample)
    print('Valid samples: %d, Other samples: %d, Invalid samples: %d' % \
        (len(valid_samples), len(other_samples), len(invalid_samples)))
    os.makedirs(root_dir)
    write_sets(root_dir, 'valid', valid_samples)
    write_sets(root_dir, 'other', other_samples)
    write_sets(root_dir, 'invalid', invalid_samples)

if __name__ == '__main__' :
    try:
        main()
    except KeyboardInterrupt:
        print('\rInterrupted by user' + ' ' * 100)