from pyflink.datastream.connectors.file_system import FileSource, StreamFormat

def get_file_source(properties: dict):
    file_uri = properties.get("fileInput.uri")

    source = FileSource \
        .for_record_stream_format(StreamFormat.text_line_format(), file_uri) \
        .build()

    return source
