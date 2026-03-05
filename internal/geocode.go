package internal

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"os"
	"strconv"
	"strings"
)

const censusBatchURL = "https://geocoding.geo.census.gov/geocoder/locations/addressbatch"

// GeocodeLogFile reads the JSONL log file, geocodes all unique hospital
// addresses via the US Census batch API, and rewrites the log file with
// geocode results populated.
func GeocodeLogFile(logFile string) error {
	entries, err := readLogEntries(logFile)
	if err != nil {
		return fmt.Errorf("read log entries: %w", err)
	}
	if len(entries) == 0 {
		return nil
	}

	// Collect unique addresses, assigning each a numeric ID.
	addrToID := make(map[string]int)
	var orderedAddrs []string
	for _, e := range entries {
		for _, addr := range e.HospitalAddresses {
			addr = strings.TrimSpace(addr)
			if addr == "" {
				continue
			}
			if _, ok := addrToID[addr]; !ok {
				addrToID[addr] = len(orderedAddrs) + 1
				orderedAddrs = append(orderedAddrs, addr)
			}
		}
	}

	if len(orderedAddrs) == 0 {
		return nil
	}

	slog.Info("geocoding", "unique_addresses", len(orderedAddrs))

	// Build CSV for Census batch API: id, street, city, state, zip
	// Put full address in street field; leave city/state/zip blank.
	var csvBuf bytes.Buffer
	for _, addr := range orderedAddrs {
		id := addrToID[addr]
		fmt.Fprintf(&csvBuf, "%d, %s, , , \n", id, addr)
	}

	// POST to Census geocoder.
	results, err := callCensusBatchAPI(csvBuf.Bytes())
	if err != nil {
		return fmt.Errorf("census geocode API: %w", err)
	}

	// Build map from address string → geocodeResult.
	idToAddr := make(map[int]string, len(orderedAddrs))
	for addr, id := range addrToID {
		idToAddr[id] = addr
	}

	geoMap := make(map[string]geocodeResult, len(orderedAddrs))
	// Initialize all as unmatched.
	for _, addr := range orderedAddrs {
		geoMap[addr] = geocodeResult{Address: addr, Matched: false}
	}

	for _, r := range results {
		addr, ok := idToAddr[r.id]
		if !ok {
			continue
		}
		geoMap[addr] = geocodeResult{
			Address:   addr,
			Matched:   r.matched,
			MatchType: r.matchType,
			Latitude:  r.lat,
			Longitude: r.lon,
		}
	}

	// Count matches.
	matched := 0
	for _, g := range geoMap {
		if g.Matched {
			matched++
		}
	}
	slog.Info("geocoded", "matched", matched, "total", len(orderedAddrs))

	// Populate geocodes on each entry.
	for i := range entries {
		var geocodes []geocodeResult
		for _, addr := range entries[i].HospitalAddresses {
			addr = strings.TrimSpace(addr)
			if addr == "" {
				continue
			}
			if g, ok := geoMap[addr]; ok {
				geocodes = append(geocodes, g)
			}
		}
		entries[i].Geocodes = geocodes
	}

	// Rewrite the log file.
	return writeLogEntries(logFile, entries)
}

type censusResult struct {
	id        int
	matched   bool
	matchType string
	lat       float64
	lon       float64
}

func callCensusBatchAPI(csvData []byte) ([]censusResult, error) {
	var body bytes.Buffer
	w := multipart.NewWriter(&body)

	if err := w.WriteField("benchmark", "Public_AR_Current"); err != nil {
		return nil, err
	}

	fw, err := w.CreateFormFile("addressFile", "addresses.csv")
	if err != nil {
		return nil, err
	}
	if _, err := fw.Write(csvData); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}

	resp, err := http.Post(censusBatchURL, w.FormDataContentType(), &body)
	if err != nil {
		return nil, fmt.Errorf("HTTP POST: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(respBody))
	}

	return parseCensusResponse(resp.Body)
}

// parseCensusResponse parses the Census batch geocoder CSV response.
// Columns: ID, InputAddress, Match/NoMatch, MatchType, OutputAddress, Lon/Lat, TigerlineID, Side
// The coordinates column is formatted as "lon,lat" (longitude first).
func parseCensusResponse(r io.Reader) ([]censusResult, error) {
	var results []censusResult
	reader := csv.NewReader(r)
	reader.FieldsPerRecord = -1 // variable number of fields

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return results, fmt.Errorf("parse CSV row: %w", err)
		}

		if len(record) < 3 {
			continue
		}

		id, err := strconv.Atoi(strings.TrimSpace(record[0]))
		if err != nil {
			continue
		}

		matchIndicator := strings.TrimSpace(record[2])
		matched := strings.EqualFold(matchIndicator, "Match")

		cr := censusResult{id: id, matched: matched}

		if matched && len(record) >= 6 {
			cr.matchType = strings.TrimSpace(record[3])
			// record[5] contains coordinates as "lon,lat"
			coords := strings.TrimSpace(record[5])
			parts := strings.SplitN(coords, ",", 2)
			if len(parts) == 2 {
				cr.lon, _ = strconv.ParseFloat(strings.TrimSpace(parts[0]), 64)
				cr.lat, _ = strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
			}
		}

		results = append(results, cr)
	}

	return results, nil
}

func readLogEntries(path string) ([]logEntry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var entries []logEntry
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var e logEntry
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			slog.Warn("skipping unparseable log line", "error", err)
			continue
		}
		entries = append(entries, e)
	}
	return entries, scanner.Err()
}

func writeLogEntries(path string, entries []logEntry) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create log file: %w", err)
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	for _, e := range entries {
		data, err := json.Marshal(e)
		if err != nil {
			return fmt.Errorf("marshal log entry: %w", err)
		}
		w.Write(data)
		w.WriteByte('\n')
	}
	return w.Flush()
}
