from typing import Self, Iterator

import av


class Decoder:
    def __init__(self, url):
        self.container = av.open(url)
        self.video_stream = None

    def __enter__(self) -> Self:
        self.container.__enter__()
        if not self.container.streams.video:
            self.container.__exit__(None, None, None)
            raise ValueError('No video stream found')
        self.video_stream = self.container.streams.video[0]
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.video_stream = None
        return self.container.__exit__(exc_type, exc_val, exc_tb)

    def __iter__(self) -> Iterator[av.VideoFrame]:
        assert self.video_stream is not None, 'Decoder must be used as a context manager'
        # noinspection PyTypeChecker
        return (
            frame
            for packet in self.container.demux(self.video_stream)
            for frame in packet.decode()
        )
