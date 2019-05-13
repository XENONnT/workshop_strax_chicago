import csv
import numpy as np
import scipy.signal
import strax
import os

export, __all__ = strax.exporter()

def records_needed(pulse_length, samples_per_record):
    """Return records needed to store pulse_length samples"""
    return np.ceil(pulse_length / samples_per_record).astype(np.int)


def rand_instructions(n=1000):
    nelectrons = 10 ** (np.random.uniform(1, 4.8, n))

    instructions = np.zeros(2 * n, dtype=[('event_number', np.int), ('type', '<U2'), ('t', np.int), ('x', np.float32),
                                          ('y', np.float32), ('z', np.float32), ('amp', np.int), ('recoil', '<U2')])
    instructions['event_number'] = np.repeat(np.arange(n), 2)
    instructions['type'] = np.tile(['s1', 's2'], n)
    instructions['t'] = np.ones(n * 2) * 1e6
    r = np.sqrt(np.random.uniform(0, 2500, n))
    t = np.random.uniform(-np.pi, np.pi, n)

    instructions['x'] = np.repeat(r * np.cos(t), 2)
    instructions['y'] = np.repeat(r * np.sin(t), 2)
    instructions['z'] = np.repeat(np.random.uniform(-100, 0, n), 2)
    instructions['amp'] = np.vstack(
        [np.random.uniform(3000, 3001, n), nelectrons]).T.flatten().astype(int)
    instructions['recoil'] = ['er' for i in range(n * 2)]

    return instructions

def simulate_pulse(event,t):
    x = np.zeros(1,dtype=[('event',np.int),('channel',np.int),('t',(np.float,100)),('signal',(np.float,100))])
    pulse_length = np.linspace(-1, 1, 100,)
    x['event'] = int(event)
    x['channel'] = np.random.randint(0,248)
    smeared_time = np.random.normal(int(t))
    x['t'] = np.linspace(smeared_time-1, smeared_time+1, 100,)
    x['signal'] = -1e3*scipy.signal.gausspulse(pulse_length, fc=5, retquad=False, retenv=True)[1]
    return x

def fax_to_peaks(file,samples_per_record,events_per_chunk):
    
    results = []

    if f.type = str():
        with open(file) as f:
            reader = csv.reader(f, delimiter=',')
            puls = list()
            for row in reader:
                if reader.line_num == 1:
                    continue
                for i in range(int(row[5]) + int(row[6])):
                    puls.extend(simulate_pulse(row[0], row[7]))
        pulses = np.zeros(len(puls), dtype=[('event', np.int), ('channel', np.int), ('time', (np.float, 100)),
                                            ('signal', (np.float, 100))])
        for i in range(len(puls)):
            pulses[i] = puls[i]


    if f.type = int()
        pulses = np.zeros(f,dtype=[('event', np.int), ('channel', np.int), ('time', (np.float, 100)),
                                            ('signal', (np.float, 100))])

        peaks = Simulator.S1(array,rand_instructions(f))





    for event in range(np.max(pulses['event'])):
        event_pulses = pulses[pulses['event']==event]

        pulse_lengths = np.array([p.size
                                  for p in event_pulses['time']])


        n_records_tot = records_needed(pulse_lengths,
                                           samples_per_record).sum()
        records = np.zeros(n_records_tot,
                           dtype=strax.record_dtype(samples_per_record))

        output_record_index = 0  # Record offset in data

        for p in event_pulses:
            n_records = records_needed(p['time'].size, samples_per_record)

            for rec_i in range(n_records):
                r = records[output_record_index]
                r['time'] = (p['time'][0]
                                 + rec_i * samples_per_record * 10)
                r['channel'] = p['channel']
                r['pulse_length'] = p['time'].size
                r['record_i'] = rec_i
                r['dt'] = 10

                    # How much are we storing in this record?
                if rec_i != n_records - 1:
                        # There's more chunks coming, so we store a full chunk
                    n_store = samples_per_record
                    assert p['time'].size > samples_per_record * (rec_i + 1)
                else:
                        # Just enough to store the rest of the data
                        # Note it's not p.length % samples_per_record!!!
                        # (that would be zero if we have to store a full record)
                    n_store = p['time'].size - samples_per_record * rec_i

                assert 0 <= n_store <= samples_per_record
                r['length'] = n_store

                offset = rec_i * samples_per_record
                r['data'][:n_store] = p['signal'][offset:offset + n_store]
                output_record_index += 1

        results.append(records)
        if len(results) >= events_per_chunk:
            yield finish_results()

    if len(results):
        y = finish_results()
        if len(y):
            yield y
    
@export
@strax.takes_config(
    strax.Option('fax_file', default=None, track=False,
                 help="Directory with fax instructions"),
    strax.Otion('nevents',default = 50,track=False,
                help="Number of random events to generate if no instructions are provided")
    strax.Option('events_per_chunk', default=50, track=False,
                 help="Number of events to yield per chunk"),
    strax.Option('samples_per_record', default=strax.DEFAULT_RECORD_LENGTH, track=False,
                 help="Number of samples per record")
    strax.Option('general_config',default='https://github..../')
)
class PeaksFromFax(strax.Plugin):
    provides = 'Peaks'
    data_kind = 'Peaks'
    compressor = 'zstd'
    depends_on = tuple()
    parallel = False
    rechunk_on_save = False

    def infer_dtype(self):
        return strax.peak_dtype(self.config['samples_per_record'])

    def iter(self, *args, **kwargs):
        if not os.path.exists(self.config['fax_file']):
            raise FileNotFoundError(self.config['fax_file'])
        if not strax.config['fax_file']:
            yield from fax_to_peaks(
                random(self.config['nevents']),)
            )

        else:
            yield from fax_to_records(
                self.config['fax_file'],)