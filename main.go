package main

import (
	"bytes"
	"context"
	"errors"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/transcribestreamingservice"
	"github.com/cryptix/wav"
	"io"
	"log"
	"os"
)

const frameSize = 10 * 1024

func main() {
	// get the file name from the command line
	if len(os.Args) < 2 {
		log.Fatal("Usage: transcribe <filename>")
	}
	filename := os.Args[1]
	log.Println("Opening file")
	stats, err := os.Stat(filename)
	if err != nil {
		log.Fatalf("failed to read stats of file, %v", err)
	}

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("failed to open file, %v", err)
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Fatalf("failed to close file, %v", err)
		}
	}(file)

	log.Println("Reading file")
	wavReader, err := wav.NewReader(file, stats.Size())
	if err != nil {
		log.Fatalf("failed to create wav reader, %v", err)
	}

	log.Println("Creating new stream for transcription")

	sess, err := session.NewSession()
	if err != nil {
		log.Fatalf("failed to load SDK configuration, %v", err)
	}

	client := transcribestreamingservice.New(sess, &aws.Config{Region: aws.String("us-east-2")})
	resp, err := client.StartStreamTranscription(&transcribestreamingservice.StartStreamTranscriptionInput{
		EnableChannelIdentification:       aws.Bool(true),
		LanguageCode:                      aws.String(transcribestreamingservice.LanguageCodeEnUs),
		MediaEncoding:                     aws.String(transcribestreamingservice.MediaEncodingPcm),
		MediaSampleRateHertz:              aws.Int64(int64(wavReader.GetSampleRate())),
		NumberOfChannels:                  aws.Int64(int64(wavReader.GetNumChannels())),
		EnablePartialResultsStabilization: aws.Bool(false),
	})
	if err != nil {
		log.Fatalf("failed to start streaming, %v", err)
	}

	stream := resp.GetStream()
	defer func(stream *transcribestreamingservice.StartStreamTranscriptionEventStream) {
		err := stream.Close()
		if err != nil {
			log.Fatalf("failed to close stream, %v", err)
		}
	}(stream)

	// prepare the buffer
	buffer := bytes.NewBuffer(make([]byte, frameSize))

	// and try to read the wav file and send it to the stream
	log.Println("Loading wav file into buffer")
loop:
	for {
		s, err := wavReader.ReadRawSample()
		if errors.Is(err, io.EOF) {
			break loop
		} else if err != nil {
			log.Fatalf("failed to read sample, %v", err)
		}
		buffer.Write(s)
	}

	go func() {
		log.Println("Starting transcription")
		err := transcribestreamingservice.StreamAudioFromReader(context.Background(), stream.Writer, frameSize, buffer)
		if err != nil {
			log.Fatalf("failed to stream audio, %v", err)
		}
	}()

	for event := range stream.Events() {
		switch e := event.(type) {
		case *transcribestreamingservice.TranscriptEvent:
			for _, res := range e.Transcript.Results {
				for _, alt := range res.Alternatives {
					log.Printf("* %s", aws.StringValue(alt.Transcript))
				}
			}
		default:
			log.Fatalf("unexpected event, %T", event)
		}
	}

	if err := stream.Err(); err != nil {
		log.Fatalf("expect no error from stream, got %v", err)
	}
}
