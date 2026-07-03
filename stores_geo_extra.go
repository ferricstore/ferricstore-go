package ferricstore

import "context"

func (s *GeoStore) Pos(ctx context.Context, key string, members ...any) (any, error) {
	args := []any{"GEOPOS", key}
	for _, member := range members {
		encoded, err := s.client.encode(member)
		if err != nil {
			return nil, err
		}
		args = append(args, encoded)
	}
	return s.client.Command(ctx, args...)
}

func (s *GeoStore) Hash(ctx context.Context, key string, members ...any) ([]string, error) {
	args := []any{"GEOHASH", key}
	for _, member := range members {
		encoded, err := s.client.encode(member)
		if err != nil {
			return nil, err
		}
		args = append(args, encoded)
	}
	value, err := s.client.Command(ctx, args...)
	return stringArray(value, err)
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
	return s.client.Command(ctx, args...)
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
	value, err := s.client.Command(ctx, args...)
	return asInt64(value), err
}

func (s *GeoStore) geoSearchArgs(command, key string, opt GeoSearchOptions) ([]any, error) {
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
