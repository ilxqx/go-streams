package streams

import (
	"bufio"
	"encoding/csv"
	"io"
	"os"
	"strings"
)

// --- Line-based IO Constructors ---

// FromReaderLines creates a Stream of lines from an io.Reader.
// Each line excludes the trailing newline character.
// The caller is responsible for closing the reader.
func FromReaderLines(r io.Reader) Stream[string] {
	return FromScanner(bufio.NewScanner(r))
}

// FromScanner creates a Stream from a bufio.Scanner.
// Each call to the scanner's Scan method yields one element.
// The caller is responsible for the scanner's lifecycle.
func FromScanner(scanner *bufio.Scanner) Stream[string] {
	return Stream[string]{
		seq: func(yield func(string) bool) {
			for scanner.Scan() {
				if !yield(scanner.Text()) {
					return
				}
			}
		},
	}
}

// FromScannerErr creates a Stream of Results from a bufio.Scanner.
// Scanner errors are yielded as Err results.
func FromScannerErr(scanner *bufio.Scanner) Stream[Result[string]] {
	return Stream[Result[string]]{
		seq: func(yield func(Result[string]) bool) {
			for scanner.Scan() {
				if !yield(Ok(scanner.Text())) {
					return
				}
			}
			if err := scanner.Err(); err != nil {
				yield(Err[string](err))
			}
		},
	}
}

// FromReaderLinesErr creates a Stream of Results from an io.Reader.
// Reader errors are yielded as Err results.
func FromReaderLinesErr(r io.Reader) Stream[Result[string]] {
	return FromScannerErr(bufio.NewScanner(r))
}

// FileLineStream represents a stream of lines from a file with resource management.
type FileLineStream struct {
	Stream[string]
	file *os.File
}

// Close closes the underlying file.
func (f *FileLineStream) Close() error {
	if f.file != nil {
		return f.file.Close()
	}
	return nil
}

// FromFileLines opens a file and creates a Stream of its lines.
// Returns the stream and a close function that must be called when done.
// Usage:
//
//	stream, err := FromFileLines("file.txt")
//	if err != nil { ... }
//	defer stream.Close()
//	for line := range stream.Seq() { ... }
func FromFileLines(path string) (*FileLineStream, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &FileLineStream{Stream: FromReaderLines(file), file: file}, nil
}

// MustFromFileLines opens a file and creates a Stream of its lines.
// Panics if the file cannot be opened.
func MustFromFileLines(path string) *FileLineStream {
	fls, err := FromFileLines(path)
	if err != nil {
		panic(err)
	}
	return fls
}

// MustFromCSVFile opens a CSV file and creates a stream of records.
// Panics if the file cannot be opened.
func MustFromCSVFile(path string) *CSVStream {
	cs, err := FromCSVFile(path)
	if err != nil {
		panic(err)
	}
	return cs
}

// MustFromTSVFile opens a TSV file and creates a stream of records.
// Panics if the file cannot be opened.
func MustFromTSVFile(path string) *CSVStream {
	cs, err := FromTSVFile(path)
	if err != nil {
		panic(err)
	}
	return cs
}

// FromStringLines creates a Stream of lines from a string.
func FromStringLines(s string) Stream[string] {
	return FromReaderLines(strings.NewReader(s))
}

// FromBytes creates a Stream of bytes from a byte slice.
func FromBytes(data []byte) Stream[byte] {
	return Stream[byte]{
		seq: func(yield func(byte) bool) {
			for _, b := range data {
				if !yield(b) {
					return
				}
			}
		},
	}
}

// FromRunes creates a Stream of runes from a string.
func FromRunes(s string) Stream[rune] {
	return Stream[rune]{
		seq: func(yield func(rune) bool) {
			for _, r := range s {
				if !yield(r) {
					return
				}
			}
		},
	}
}

// --- CSV/TSV Constructors ---
//
// Error handling strategy:
//   - Non-Err variants (FromCSV, FromTSV, etc.): Terminate stream on first parse error.
//     Use these when you expect well-formed input and want fail-fast behavior.
//   - Err variants (FromCSVErr, FromTSVErr, etc.): Yield errors as Result[T] and continue.
//     Use these when you need to handle or skip malformed records gracefully.

// CSVStream represents a stream of CSV records with resource management.
type CSVStream struct {
	Stream[[]string]
	reader *csv.Reader
	closer io.Closer
}

// Close closes the underlying reader if it implements io.Closer.
func (c *CSVStream) Close() error {
	if c.closer != nil {
		return c.closer.Close()
	}
	return nil
}

// FromCSV creates a Stream of CSV records (each record is a []string).
// Parse errors terminate the stream silently. Use FromCSVErr for explicit error handling.
// The caller is responsible for closing the reader.
func FromCSV(r io.Reader) Stream[[]string] {
	return fromDelimited(r, ',')
}

// fromDelimited creates a Stream of delimited records (comma or tab).
func fromDelimited(r io.Reader, delim rune) Stream[[]string] {
	csvReader := csv.NewReader(r)
	csvReader.Comma = delim
	return Stream[[]string]{
		seq: func(yield func([]string) bool) {
			for {
				record, err := csvReader.Read()
				if err == io.EOF {
					return
				}
				if err != nil {
					return
				}
				if !yield(record) {
					return
				}
			}
		},
	}
}

// FromCSVErr creates a Stream of CSV records with error handling.
// Parse errors are yielded as Err results, and parsing continues with the next record.
// This allows handling malformed records without terminating the stream.
func FromCSVErr(r io.Reader) Stream[Result[[]string]] {
	return fromDelimitedErr(r, ',')
}

// fromDelimitedErr creates a Stream of delimited records with error handling.
func fromDelimitedErr(r io.Reader, delim rune) Stream[Result[[]string]] {
	csvReader := csv.NewReader(r)
	csvReader.Comma = delim
	return Stream[Result[[]string]]{
		seq: func(yield func(Result[[]string]) bool) {
			for {
				record, err := csvReader.Read()
				if err == io.EOF {
					return
				}
				if err != nil {
					if !yield(Err[[]string](err)) {
						return
					}
					continue
				}
				if !yield(Ok(record)) {
					return
				}
			}
		},
	}
}

// fromDelimitedFile opens a file and creates a stream of delimited records.
func fromDelimitedFile(path string, delim rune) (*CSVStream, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	csvReader := csv.NewReader(file)
	csvReader.Comma = delim
	stream := Stream[[]string]{
		seq: func(yield func([]string) bool) {
			for {
				record, err := csvReader.Read()
				if err == io.EOF {
					return
				}
				if err != nil {
					return
				}
				if !yield(record) {
					return
				}
			}
		},
	}
	return &CSVStream{Stream: stream, reader: csvReader, closer: file}, nil
}

// FromCSVFile opens a CSV file and creates a stream of records.
// Parse errors terminate the stream silently. For error handling, use FromCSVErr with manual file open.
func FromCSVFile(path string) (*CSVStream, error) {
	return fromDelimitedFile(path, ',')
}

// FromTSV creates a Stream of TSV (tab-separated) records.
// Parse errors terminate the stream silently. Use FromTSVErr for explicit error handling.
func FromTSV(r io.Reader) Stream[[]string] {
	return fromDelimited(r, '\t')
}

// FromTSVFile opens a TSV file and creates a stream of records.
// Parse errors terminate the stream silently. For error handling, use FromTSVErr with manual file open.
func FromTSVFile(path string) (*CSVStream, error) {
	return fromDelimitedFile(path, '\t')
}

// FromTSVErr creates a Stream of TSV records with error handling.
// Parse errors are yielded as Err results, and parsing continues with the next record.
func FromTSVErr(r io.Reader) Stream[Result[[]string]] {
	return fromDelimitedErr(r, '\t')
}

// CSVRecord represents a single CSV record with named fields.
type CSVRecord map[string]string

// Get returns the value for a field, or empty string if not found.
func (r CSVRecord) Get(field string) string {
	return r[field]
}

// GetOr returns the value for a field, or the default if not found.
func (r CSVRecord) GetOr(field, defaultVal string) string {
	if v, ok := r[field]; ok {
		return v
	}
	return defaultVal
}

// FromCSVWithHeader creates a Stream of CSVRecords using the first row as headers.
// Parse errors terminate the stream silently. Use FromCSVWithHeaderErr for explicit error handling.
func FromCSVWithHeader(r io.Reader) Stream[CSVRecord] {
	csvReader := csv.NewReader(r)
	return Stream[CSVRecord]{
		seq: func(yield func(CSVRecord) bool) {
			header, err := csvReader.Read()
			if err != nil {
				return
			}

			for {
				record, err := csvReader.Read()
				if err == io.EOF {
					return
				}
				if err != nil {
					return
				}

				row := make(CSVRecord, len(header))
				for i, h := range header {
					if i < len(record) {
						row[h] = record[i]
					}
				}

				if !yield(row) {
					return
				}
			}
		},
	}
}

// FromCSVWithHeaderErr creates a Stream of CSVRecords with error handling.
// Parse errors are yielded as Err results, and parsing continues with the next record.
func FromCSVWithHeaderErr(r io.Reader) Stream[Result[CSVRecord]] {
	csvReader := csv.NewReader(r)
	return Stream[Result[CSVRecord]]{
		seq: func(yield func(Result[CSVRecord]) bool) {
			header, err := csvReader.Read()
			if err != nil {
				yield(Err[CSVRecord](err))
				return
			}

			for {
				record, err := csvReader.Read()
				if err == io.EOF {
					return
				}
				if err != nil {
					if !yield(Err[CSVRecord](err)) {
						return
					}
					continue
				}

				row := make(CSVRecord, len(header))
				for i, h := range header {
					if i < len(record) {
						row[h] = record[i]
					}
				}

				if !yield(Ok(row)) {
					return
				}
			}
		},
	}
}

// --- Writer Utilities ---

// ToWriter writes stream elements to an io.Writer, one per line.
// Note: A newline is automatically appended after each formatted element.
// The provided format function should NOT include a trailing newline.
func ToWriter[T any](s Stream[T], w io.Writer, format func(T) string) error {
	bw := bufio.NewWriter(w)
	for v := range s.seq {
		if _, err := bw.WriteString(format(v)); err != nil {
			return err
		}
		if _, err := bw.WriteString("\n"); err != nil {
			return err
		}
	}
	return bw.Flush()
}

// ToFile writes stream elements to a file, one per line.
func ToFile[T any](s Stream[T], path string, format func(T) string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()
	return ToWriter(s, file, format)
}

// ToCSV writes a stream of string slices as CSV to a writer.
func ToCSV(s Stream[[]string], w io.Writer) error {
	csvWriter := csv.NewWriter(w)
	defer csvWriter.Flush()

	for record := range s.seq {
		if err := csvWriter.Write(record); err != nil {
			return err
		}
	}
	return csvWriter.Error()
}

// ToCSVFile writes a stream of string slices as CSV to a file.
func ToCSVFile(s Stream[[]string], path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()
	return ToCSV(s, file)
}
