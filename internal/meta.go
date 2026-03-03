package internal

// RunMeta contains hospital metadata extracted from MRF headers,
// exposed for logging and external consumption.
type RunMeta struct {
	HospitalName      string
	LocationNames     []string
	HospitalAddresses []string
	LicenseNumber     *string
	LicenseState      *string
	Type2NPIs         []string
	LastUpdatedOn     string
	Version           string
}
