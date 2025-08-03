namespace Domain.Common;

public class ChunkStream : Stream {
    private readonly FileStream _fileStream;
    private readonly long _startPosition;
    private readonly long _length;
    private long _position;

    public ChunkStream(string filePath, long offset, long size, int bufferSize = 4096) {
        _fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize,
            FileOptions.SequentialScan);
        _startPosition = offset;
        _length = size;
        _position = 0;

        _fileStream.Seek(offset, SeekOrigin.Begin);
    }

    public override bool CanRead => true;
    public override bool CanSeek => true;
    public override bool CanWrite => false;
    public override long Length => _length;

    public override long Position {
        get => _position;
        set => Seek(value, SeekOrigin.Begin);
    }

    public override int Read(byte[] buffer, int offset, int count) {
        if (_position >= _length)
            return 0;

        // Don't read beyond the chunk boundary
        var bytesToRead = (int)Math.Min(count, _length - _position);
        var bytesRead = _fileStream.Read(buffer, offset, bytesToRead);

        _position += bytesRead;
        return bytesRead;
    }

    public override int Read(Span<byte> buffer) {
        if (_position >= _length)
            return 0;

        var bytesToRead = (int)Math.Min(buffer.Length, _length - _position);
        var bytesRead = _fileStream.Read(buffer[..bytesToRead]);

        _position += bytesRead;
        return bytesRead;
    }
    public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = new CancellationToken()) {
        if (_position >= _length)
            return 0;

        var bytesToRead = (int)Math.Min(buffer.Length, _length - _position);
        var bytesRead = await _fileStream.ReadAsync(buffer[..bytesToRead], cancellationToken);

        _position += bytesRead;
        return bytesRead;
    }
    public override async Task<int> ReadAsync(byte[] buffer, int offset, int count,
        System.Threading.CancellationToken cancellationToken) {
        if (_position >= _length)
            return 0;

        var bytesToRead = (int)Math.Min(count, _length - _position);
        var bytesRead = await _fileStream.ReadAsync(buffer.AsMemory(offset, bytesToRead), cancellationToken);

        _position += bytesRead;
        return bytesRead;
    }

    public override long Seek(long offset, SeekOrigin origin) {
        var newPosition = origin switch {
            SeekOrigin.Begin => offset,
            SeekOrigin.Current => _position + offset,
            SeekOrigin.End => _length + offset,
            _ => throw new ArgumentException("Invalid seek origin")
        };

        if (newPosition < 0 || newPosition > _length)
            throw new ArgumentOutOfRangeException(nameof(offset), "Seek position is outside chunk bounds");

        _position = newPosition;
        _fileStream.Seek(_startPosition + newPosition, SeekOrigin.Begin);
        return _position;
    }

    public override void Flush() => _fileStream.Flush();
    public override void SetLength(long value) => throw new NotSupportedException();
    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

    protected override void Dispose(bool disposing) {
        if (disposing) {
            _fileStream?.Dispose();
        }

        base.Dispose(disposing);
    }
}