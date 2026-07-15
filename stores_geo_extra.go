package ferricstore

import (
	"context"
	"errors"
)

func (s *GeoStore) Pos(ctx context.Context, key string, members ...any) (any, error) {
	if len(members) == 0 {
		return nil, nil
	}
	args := []any{"GEOPOS", key}
	for _, member := range members {
		encoded, err := s.client.encode(member)
		if err != nil {
			return nil, err
		}
		args = append(args, encoded)
	}
	value, err := s.client.typedReply(ctx, args...)
	return validateGeoPositionResponse(value, err, len(members))
}

func (s *GeoStore) Hash(ctx context.Context, key string, members ...any) ([]string, error) {
	if len(members) == 0 {
		return nil, nil
	}
	args := []any{"GEOHASH", key}
	for _, member := range members {
		encoded, err := s.client.encode(member)
		if err != nil {
			return nil, err
		}
		args = append(args, encoded)
	}
	value, err := s.client.typedReply(ctx, args...)
	return stringArrayExact(value, err, len(members), "GEOHASH")
}

type GeoSearchOptions struct {
	FromMember any
	FromLonLat *GeoCoordinate
	ByRadius   *GeoRadius
	ByBox      *GeoBox
	Asc        bool
	Desc       bool
	Count      *int
	Any        bool
	WithCoord  bool
	WithDist   bool
	WithHash   bool
}

type GeoCoordinate struct {
	Longitude float64
	Latitude  float64
}

type GeoRadius struct {
	Radius float64
	Unit   string
}

type GeoBox struct {
	Width  float64
	Height float64
	Unit   string
}

func (s *GeoStore) Search(ctx context.Context, key string, opt GeoSearchOptions) (any, error) {
	args, err := s.geoSearchArgs("GEOSEARCH", key, opt)
	if err != nil {
		return nil, err
	}
	value, err := s.client.typedReply(ctx, args...)
	metadataFields := boolInt(opt.WithCoord) + boolInt(opt.WithDist) + boolInt(opt.WithHash)
	return decodeGeoSearch(s.client.codec, value, err, metadataFields)
}

func (s *GeoStore) SearchStore(ctx context.Context, destination, source string, opt GeoSearchOptions, storeDist bool) (int64, error) {
	args, err := s.geoSearchArgs("GEOSEARCHSTORE", destination, opt)
	if err != nil {
		return 0, err
	}
	args = append(args[:2], append([]any{source}, args[2:]...)...)
	if storeDist {
		args = append(args, "STOREDIST")
	}
	value, err := s.client.typedReply(ctx, args...)
	return nonNegativeInt64Response("GEOSEARCHSTORE", value, err)
}

func (s *GeoStore) geoSearchArgs(command, key string, opt GeoSearchOptions) ([]any, error) {
	if boolInt(opt.FromMember != nil)+boolInt(opt.FromLonLat != nil) != 1 {
		return nil, errors.New("GEOSEARCH requires exactly one of FROMMEMBER or FROMLONLAT")
	}
	if boolInt(opt.ByRadius != nil)+boolInt(opt.ByBox != nil) != 1 {
		return nil, errors.New("GEOSEARCH requires exactly one of BYRADIUS or BYBOX")
	}
	if opt.Asc && opt.Desc {
		return nil, errors.New("GEOSEARCH ASC and DESC are mutually exclusive")
	}
	if opt.Any && opt.Count == nil {
		return nil, errors.New("GEOSEARCH ANY requires COUNT")
	}
	if command == "GEOSEARCHSTORE" && (opt.WithCoord || opt.WithDist || opt.WithHash) {
		return nil, errors.New("GEOSEARCHSTORE does not accept WITHCOORD, WITHDIST, or WITHHASH")
	}
	if opt.FromLonLat != nil {
		if err := validateGeoCoordinate(opt.FromLonLat.Longitude, opt.FromLonLat.Latitude); err != nil {
			return nil, err
		}
	}
	if opt.ByRadius != nil {
		if err := validatePositiveFinite(command, "radius", opt.ByRadius.Radius); err != nil {
			return nil, err
		}
		if err := validateGeoUnit(opt.ByRadius.Unit, false); err != nil {
			return nil, err
		}
	}
	if opt.ByBox != nil {
		if err := validatePositiveFinite(command, "box width", opt.ByBox.Width); err != nil {
			return nil, err
		}
		if err := validatePositiveFinite(command, "box height", opt.ByBox.Height); err != nil {
			return nil, err
		}
		if err := validateGeoUnit(opt.ByBox.Unit, false); err != nil {
			return nil, err
		}
	}
	if opt.Count != nil && *opt.Count <= 0 {
		return nil, errors.New("GEOSEARCH count must be positive")
	}
	args := []any{command, key}
	if opt.FromMember != nil {
		encoded, err := s.client.encode(opt.FromMember)
		if err != nil {
			return nil, err
		}
		args = append(args, "FROMMEMBER", encoded)
	}
	if opt.FromLonLat != nil {
		args = append(args, "FROMLONLAT", opt.FromLonLat.Longitude, opt.FromLonLat.Latitude)
	}
	if opt.ByRadius != nil {
		args = append(args, "BYRADIUS", opt.ByRadius.Radius, opt.ByRadius.Unit)
	}
	if opt.ByBox != nil {
		args = append(args, "BYBOX", opt.ByBox.Width, opt.ByBox.Height, opt.ByBox.Unit)
	}
	if opt.Asc {
		args = append(args, "ASC")
	}
	if opt.Desc {
		args = append(args, "DESC")
	}
	if opt.Count != nil {
		args = append(args, "COUNT", *opt.Count)
		if opt.Any {
			args = append(args, "ANY")
		}
	}
	if opt.WithCoord {
		args = append(args, "WITHCOORD")
	}
	if opt.WithDist {
		args = append(args, "WITHDIST")
	}
	if opt.WithHash {
		args = append(args, "WITHHASH")
	}
	return args, nil
}
