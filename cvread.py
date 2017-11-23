#!/usr/bin/env python
from __future__ import absolute_import, division, print_function
import os
import sys
import csv
import glob
import json
import subprocess
from threading import Lock
from multiprocessing.dummy import Pool
from multiprocessing import cpu_count

class Client(object):
    def __init__(self, id):
        self.id = id
        self.age = None
        self.accent = None
        self.gender = None
        self.samples = []
    
    def __str__(self):
        return 'ID: %s, num_samples: %d' % (self.id, len(self.samples))

class Sample(object):
    def __init__(self, client, id):
        self.client = client
        self.id = id
        self.text = None
        self.filename = None
        self.up_votes = 0
        self.down_votes = 0

    def __str__(self):
        return 'ID:   %s\nText: %s\nUp:   %d\nDown: %d' % (self.id, self.text, self.up_votes, self.down_votes)

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

def main():
    clients = {}
    samples = {}
    root_dir = os.path.abspath(sys.argv[1])
    csv_path = os.path.abspath(sys.argv[2]) if len(sys.argv) > 2 else 'samples.csv'
    paths = glob.glob(root_dir + '/*')
    print('Reading meta data of samples in directory "%s"...' % root_dir)
    _print_progress(0, len(paths))
    for i, client_dir in enumerate(paths):
        _print_progress(i + 1, len(paths))
        if not os.path.isdir(client_dir):
            continue
        client_id = client_dir.split('/')[-1]
        client = Client(client_id)
        clients[client_id] = client
        for file_path in glob.glob(client_dir + '/*'):
            if os.path.isdir(file_path):
                continue
            filename = file_path.split('/')[-1].lower()
            name, ext = tuple(filename.split('.'))
            #print('name: %s, ext: %s' % (name, ext))
            if ext == 'json':
                with open(file_path, 'r') as f:
                    jd = json.loads(f.read())
                client.age = jd['age']
                client.accent = jd['accent']
                client.gender = jd['gender']
                continue
            sample_id = client_id + '-' + name.split('-')[0]
            if sample_id in samples:
                sample = samples[sample_id]
            else:
                sample = Sample(client, sample_id)
                client.samples.append(sample)
                samples[sample_id] = sample
            if ext == 'vote':
                with open(file_path, 'r') as f:
                    if f.read().strip().lower() == 'true':
                        sample.up_votes += 1
                    else:
                        sample.down_votes += 1
            elif ext == 'txt':
                with open(file_path, 'r') as f:
                    sample.text = f.read().strip()
            elif ext == 'mp3':
                sample.filename = file_path
    print('Writing samples to "%s"...' % csv_path)
    with open(csv_path, 'w') as csvfile:
        fieldnames = ['filename', 'text', 'up_votes', 'down_votes', 'age', 'gender', 'accent', 'duration']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        counter = { 'value': 0, 'bad': 0 }
        lock = Lock()
        items = samples.items()
        num_samples = len(items)

        _print_progress(0, num_samples)
        def one_sample(item):
            _, sample = item
            with lock:
                counter['value'] += 1
                _print_progress(counter['value'], num_samples)
            try:
                duration = float(subprocess.check_output(['soxi', '-D', sample.filename], stderr=subprocess.STDOUT))
            except Exception as ex:
                counter['bad'] += 1
                return
            writer.writerow({ 'filename':    sample.filename,
                              'text':        sample.text,
                              'up_votes':    sample.up_votes,
                              'down_votes':  sample.down_votes,
                              'age':         sample.client.age,
                              'gender':      sample.client.gender,
                              'accent':      sample.client.accent,
                              'duration':    duration })

        pool = Pool(cpu_count())
        pool.map(one_sample, items)
        pool.close()
        pool.join()

        print('Skipped %d missing or broken samples.' % counter['bad'])

if __name__ == '__main__' :
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted by user')