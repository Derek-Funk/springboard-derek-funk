import h5py
import numpy as np

file_name = 'MillionSongSubset/data/A/A/A/TRAAAAW128F429D538.h5'

h5 = h5py.File(file_name,'r')

h5.keys()

h5['analysis'].keys()
data_songs = np.array(h5['analysis/songs'])
len(data_songs[0])

h5['metadata'].keys()
np.array(h5['metadata/similar_artists'])


h5['musicbrainz'].keys()
np.array(h5['musicbrainz/songs'])[0][1]

bars_confidence = h5.get('analysis').get('bars_confidence')
np.array(bars_confidence)

similar_artists = h5['metadata'].get('similar_artists')
np.array(similar_artists)

h5.close()