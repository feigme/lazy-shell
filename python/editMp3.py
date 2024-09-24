from mutagen.easyid3 import EasyID3

audio = EasyID3('/Users/guohui/Documents/yy/mp3/崔子格-卜卦.mp3')
audio['title'] = '卜卦'
audio['artist'] = '崔子格'
audio['album'] = '崔子格'
audio.save()