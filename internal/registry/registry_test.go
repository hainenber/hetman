package registry

import (
	"reflect"
	"testing"
)

func TestGetRegistry(t *testing.T) {
	type args struct {
		regDir string
	}
	tests := []struct {
		name    string
		args    args
		want    Registry
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GetRegistry(tt.args.regDir)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetRegistry() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetRegistry() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSaveLastPosition(t *testing.T) {
	type args struct {
		regDir            string
		lastReadPositions map[string]int64
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := SaveLastPosition(tt.args.regDir, tt.args.lastReadPositions); (err != nil) != tt.wantErr {
				t.Errorf("SaveLastPosition() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
