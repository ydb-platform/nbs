
ffmpeg -f lavfi -i testsrc=duration=1:size=1080x1080:rate=30 -c:v rawvideo  testsrc.mpg
ffmpeg -f rawvideo -pix_fmt rgb24 -video_size 1080x1080  -i testsrc.mpg -r 30  -c:v rawvideo -pix_fmt rgb24 -f caca -
