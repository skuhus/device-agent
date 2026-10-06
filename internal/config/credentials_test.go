package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeCredentials(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "credentials")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write credentials: %v", err)
	}
	return path
}

func TestLoadCredentialsFromFile(t *testing.T) {
	path := writeCredentials(t, "# station credentials\n\nusername = station-pack-03\npassword=s3cret\n")
	got, err := LoadCredentials(Broker{CredentialsFile: path}, nil)
	if err != nil {
		t.Fatalf("LoadCredentials: %v", err)
	}
	if got.Username != "station-pack-03" {
		t.Errorf("username = %q, want station-pack-03", got.Username)
	}
	if got.Password != "s3cret" {
		t.Errorf("password = %q, want s3cret", got.Password)
	}
}

// A trailing space in a password is part of the password. Trimming it produces
// an authentication failure that looks like a broker fault.
func TestLoadCredentialsKeepsPasswordWhitespace(t *testing.T) {
	path := writeCredentials(t, "username=u\npassword=two words \n")
	got, err := LoadCredentials(Broker{CredentialsFile: path}, nil)
	if err != nil {
		t.Fatalf("LoadCredentials: %v", err)
	}
	if got.Password != "two words " {
		t.Errorf("password = %q, want %q", got.Password, "two words ")
	}
}

// A credentials file written on Windows must not leave a carriage return in
// the password.
func TestLoadCredentialsHandlesCRLF(t *testing.T) {
	path := writeCredentials(t, "username=u\r\npassword=p\r\n")
	got, err := LoadCredentials(Broker{CredentialsFile: path}, nil)
	if err != nil {
		t.Fatalf("LoadCredentials: %v", err)
	}
	if got.Username != "u" || got.Password != "p" {
		t.Errorf("got %+v, want the values without carriage returns", got)
	}
}

func TestLoadCredentialsEnvironmentWins(t *testing.T) {
	path := writeCredentials(t, "username=from-file\npassword=from-file\n")
	env := map[string]string{EnvMQTTUsername: "from-env", EnvMQTTPassword: "env-secret"}
	got, err := LoadCredentials(Broker{CredentialsFile: path}, env)
	if err != nil {
		t.Fatalf("LoadCredentials: %v", err)
	}
	if got.Username != "from-env" || got.Password != "env-secret" {
		t.Errorf("got %+v, want the environment values", got)
	}
}

func TestLoadCredentialsEnvironmentOnly(t *testing.T) {
	env := map[string]string{EnvMQTTUsername: "u", EnvMQTTPassword: "p"}
	if _, err := LoadCredentials(Broker{}, env); err != nil {
		t.Fatalf("LoadCredentials with no file: %v", err)
	}
}

func TestLoadCredentialsRejections(t *testing.T) {
	tests := []struct {
		name string
		body string
		env  map[string]string
		want string
	}{
		{
			name: "unknown key",
			body: "username=u\npassword=p\nport=1883\n",
			want: `unknown key "port"`,
		},
		{
			name: "line without a separator",
			body: "username=u\njust-a-password\n",
			want: "expected key=value",
		},
		{
			name: "no password anywhere",
			body: "username=u\n",
			want: "no broker password",
		},
		{
			name: "password but no username",
			body: "password=p\n",
			want: "no broker username",
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			path := writeCredentials(t, testCase.body)
			_, err := LoadCredentials(Broker{CredentialsFile: path}, testCase.env)
			if err == nil {
				t.Fatal("expected an error")
			}
			if !strings.Contains(err.Error(), testCase.want) {
				t.Errorf("error = %v, want it to mention %q", err, testCase.want)
			}
		})
	}
}

func TestLoadCredentialsMissingFile(t *testing.T) {
	_, err := LoadCredentials(Broker{CredentialsFile: filepath.Join(t.TempDir(), "absent")}, nil)
	if err == nil {
		t.Fatal("expected an error for a missing credentials file")
	}
	if !strings.Contains(err.Error(), "credentials_file") {
		t.Errorf("error = %v, want it to name the field", err)
	}
}

// A missing username or password is reported with the places it can be set:
// the credentials file only when one is configured, so that the message never
// names an empty file.
func TestLoadCredentialsNamesWhereToSetWhatIsMissing(t *testing.T) {
	_, err := LoadCredentials(Broker{}, map[string]string{EnvMQTTPassword: "s3cret"})
	if err == nil || !strings.Contains(err.Error(), "no broker username: set "+EnvMQTTUsername+";") || strings.Contains(err.Error(), "  ") {
		t.Errorf("without a file: err = %v, want the variable alone named", err)
	}
	path := writeCredentials(t, "password=s3cret\n")
	_, err = LoadCredentials(Broker{CredentialsFile: path}, nil)
	if err == nil || !strings.Contains(err.Error(), "no broker username: set it in "+path+" or set "+EnvMQTTUsername+";") {
		t.Errorf("with a file: err = %v, want the file and the variable named", err)
	}
	_, err = LoadCredentials(Broker{}, map[string]string{EnvMQTTUsername: "station-pack-03"})
	if err == nil || err.Error() != "no broker password: set "+EnvMQTTPassword {
		t.Errorf("no password, no file: err = %v, want the variable alone named", err)
	}
}
