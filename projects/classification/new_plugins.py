import numba
import numpy as np
import strax

export, __all__ = strax.exporter()

@export
@strax.takes_config(
    strax.Option('tight_coincidence_window_left', default=50,
                 help="Time range left of peak center to call a hit a tight coincidence (ns)"),
    strax.Option('tight_coincidence_window_right', default=50,
                 help="Time range right of peak center to call a hit a tight coincidence (ns)"),
)
class TightCoincidence(strax.Plugin):
    """Calculates the tight coincidence"""
    
    # Name of the data type this plugin provides
    provides = 'tight_coincidence'
    data_kind = 'peaks'
    
    depends_on = ('records', 'peaks')
    
    # Numpy datatype of the output 
    dtype = [
        (('Hits within tight range of mean', 'tight_coincidence'), np.int16),
    ]
    
    # Version of the plugin. Increment this if you change the algorithm.
    __version__ = '0.0.1'
        
    #parallel = 'process'
    #rechunk_on_save = True
    
    @staticmethod
    @numba.jit(nopython=True, nogil=True, cache=True)
    def get_tight_coin(hit_mean_times, peak_mean_times, left, right):
        left_hit_i = 0
        n_coin = np.zeros(len(peak_mean_times), dtype=np.int16)
        
        # loop over peaks
        for p_i, p_t in enumerate(peak_mean_times):
            
            # loop over hits starting from the last one we left at
            for left_hit_i in range(left_hit_i, len(hit_mean_times)):
                
                # if the hit is in the window, its a tight coin
                if (
                    (p_t - hit_mean_times[left_hit_i] < left)
                    & (hit_mean_times[left_hit_i] - p_t < right)
                ):
                    n_coin[p_i] += 1
                    
                # stop the loop when we know we're outside the range
                if (hit_mean_times[left_hit_i] - p_t > right):
                    break
                    
        return n_coin


    def compute(self, records, peaks):
        r = records
        p = peaks
        hits = strax.find_hits(r)
        hits = strax.sort_by_time(hits)
        
        hit_mean_times = hits['time'] + (hits['length']/2.0)  # hit "mean" time
        peak_mean_times = p['time'] + (p['length']/2.0)  # peak "mean" time
        
        tight_coin = self.get_tight_coin(
            hit_mean_times, 
            peak_mean_times, 
            self.config['tight_coincidence_window_left'], 
            self.config['tight_coincidence_window_right']
        )
        
        return dict(tight_coincidence=tight_coin)

@export
@strax.takes_config(
    strax.Option('s1_max_width', default=300,
                 help="Maximum (IQR) width of S1s"),
    strax.Option('s1_min_tight_coincidence', default=3,
                 help="Maximum (IQR) width of S1s"),
    strax.Option('s1_rise_time_threshold', default=70,
                 help="Minimum time between 10p and 50p area"),
    strax.Option('s1_area_fraction_top_threshold', default=0.7,
                 help="Minimum area on top PMTs for S1s"),
    strax.Option('s1_maximum_threshold', default=0.03,
                 help="Minimum maximum for S1s"),
    strax.Option('s2_min_area', default=15,
                 help="Minimum area (PE) for S2s"),
    strax.Option('s2_min_width', default=67,
                help="Minimum width for S2s"),
)
class WorkshopClassification(strax.Plugin):
    """Everything is an S1!"""
    
    # Name of the data type this plugin provides
    provides = 'peak_classification'
    
    # Data types this plugin requires. Note we don't specify
    # what plugins should produce them: maybe the default PeakBasics
    # has been replaced by another AdvancedExpertBlabla plugin?
    depends_on = ('peaks', 'peak_basics', 'tight_coincidence', 'peak_height')
    parallel = True
    
    # Numpy datatype of the output 
    dtype = straxen.plugins.plugins.PeakClassification.dtype
    
    # Version of the plugin. Increment this if you change the algorithm.
    __version__ = '0.0.1'

    def compute(self, peaks):
        p = peaks
        r = np.zeros(len(p), dtype=self.dtype)

        #is_s1 = p['n_channels'] >= self.config['s1_min_n_channels']
        is_s1 = p['tight_coincidence'] >= self.config['s1_min_tight_coincidence']  # removes some dark rate / AC
        is_s1 &= p['range_50p_area'] < self.config['s1_max_width']  # removes some SE's
        is_s1 &= -1*p['area_decile_from_midpoint'][:,1] < self.config['s1_rise_time_threshold']  # removes some SE's
        is_s1 &= p['area_fraction_top'] < self.config['s1_area_fraction_top_threshold']  # removes some gas peaks
        is_s1 &= p['maxheight'] > self.config['s1_maximum_threshold']  # removes low, flat noisy population
        r['type'][is_s1] = 1

        is_s2 = p['area'] > self.config['s2_min_area']
        is_s2 &= p['range_50p_area'] > self.config['s2_min_width']
        #is_s2 &= (p['range_50p_area'] > f(p['area'])) | (p['area'] > self.config['s2_flat_area_threshold'])
        r['type'][is_s2] = 2
        
        return r

    
@export 
class PeakHeight(strax.Plugin):
    __version__ = "0.0.2"
    parallel = True
    depends_on = ('peaks',)
    dtype = [
        (('Maximum height of the peak waveform in PE/ns',
        'maxheight'), np.float32),
        (('Time of Maximum height of the peak waveform in ns',
        'time_maxheight'), np.int32),
        ]

    def compute(self, peaks):
        r = np.zeros(len(peaks), self.dtype)     
        for i,peak in enumerate(peaks):
            n=peak['length']
            dt = peak['dt']
            data = peak['data']
            hnorm =data[:n]/dt
            r['maxheight'][i] = np.max(hnorm)
            r['time_maxheight'][i] = np.argmax(hnorm) * dt
            
        return r
