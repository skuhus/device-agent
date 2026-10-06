package config

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// Credentials are the broker username and password.
//
// They are deliberately not part of Config: Config is logged, printed by
// validate and round-tripped through YAML, and a password has no business in
// any of that.
type Credentials struct {
	Username string
	Password string
}

// LoadCredentials resolves the broker credentials from the environment and the
// credentials file, environment first, matching the precedence in section 9.
//
// It returns an error when no password can be found, when the file cannot be
// read, or when the file contains a key it does not recognise. A silently
// empty password produces an authentication failure at the broker that reads
// like a broker problem rather than a configuration one.
func LoadCredentials(broker Broker, env map[string]string) (Credentials, error) {
	var credentials Credentials
	if broker.CredentialsFile != "" {
		fromFile, err := readCredentialsFile(broker.CredentialsFile)
		if err != nil {
			return Credentials{}, err
		}
		credentials = fromFile
	}
	if username, ok := env[EnvMQTTUsername]; ok {
		credentials.Username = username
	}
	if password, ok := env[EnvMQTTPassword]; ok {
		credentials.Password = password
	}
	if credentials.Password == "" {
		return Credentials{}, fmt.Errorf("no broker password: %s", whereToSet(broker.CredentialsFile, EnvMQTTPassword))
	}
	if credentials.Username == "" {
		return Credentials{}, fmt.Errorf(
			"no broker username: %s; section 8 requires per-station credentials, so there is no anonymous fallback",
			whereToSet(broker.CredentialsFile, EnvMQTTUsername))
	}
	return credentials, nil
}

// whereToSet names where a missing credential can be set: the credentials
// file, when one is configured, and the environment variable.
func whereToSet(file, variable string) string {
	if file == "" {
		return "set " + variable
	}
	return "set it in " + file + " or set " + variable
}

// readCredentialsFile parses key=value lines. The format is key=value rather
// than two bare lines so that a file edited by hand cannot silently swap the
// username and the password.
func readCredentialsFile(path string) (Credentials, error) {
	file, err := os.Open(path)
	if err != nil {
		return Credentials{}, fmt.Errorf("broker.credentials_file: %w", err)
	}
	defer file.Close()

	var credentials Credentials
	scanner := bufio.NewScanner(file)
	for line := 1; scanner.Scan(); line++ {
		// The raw line is what gets split, because the password is everything
		// after the first "=" and trimming the line would silently edit it.
		// A CRLF file is safe: bufio's line scanner drops the carriage return.
		raw := scanner.Text()
		if trimmed := strings.TrimSpace(raw); trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		key, value, found := strings.Cut(raw, "=")
		if !found {
			return Credentials{}, fmt.Errorf("%s line %d: expected key=value, got %q", path, line, strings.TrimSpace(raw))
		}
		switch strings.ToLower(strings.TrimSpace(key)) {
		case "username":
			credentials.Username = strings.TrimSpace(value)
		case "password":
			// Taken verbatim. A password may legitimately begin or end with a
			// space, and trimming one produces an authentication failure at the
			// broker that reads as a broker fault rather than a typo here.
			credentials.Password = value
		default:
			return Credentials{}, fmt.Errorf("%s line %d: unknown key %q; expected username or password",
				path, line, strings.TrimSpace(key))
		}
	}
	if err := scanner.Err(); err != nil {
		return Credentials{}, fmt.Errorf("%s: %w", path, err)
	}
	return credentials, nil
}
