package ferricstore

type topologyKeyMode uint8

const (
	topologyKeysNone topologyKeyMode = iota
	topologyKeysFirst
	topologyKeysAll
	topologyKeysAlternating
	topologyKeysFirstTwo
	topologyKeysBitOp
	topologyKeysCountedRead
	topologyKeysCountedStore
	topologyKeysBlockingCounted
	topologyKeysBlockingTrailing
	topologyKeysStreams
	topologyKeysSubcommandThird
	topologyKeysHelpAwareThird
	topologyKeysMemoryUsage
)

type topologyCommandPolicy struct {
	keyMode         topologyKeyMode
	requireSameSlot bool
	scatter         bool
	destructive     bool
}

var topologyCommandPolicies = map[string]topologyCommandPolicy{
	"MGET":   {keyMode: topologyKeysAll, scatter: true},
	"DEL":    {keyMode: topologyKeysAll, scatter: true, destructive: true},
	"EXISTS": {keyMode: topologyKeysAll, scatter: true},
	"UNLINK": {keyMode: topologyKeysAll, scatter: true, destructive: true},
	"TOUCH":  {keyMode: topologyKeysAll, scatter: true},

	"MSET":     {keyMode: topologyKeysAlternating, requireSameSlot: true},
	"MSETNX":   {keyMode: topologyKeysAlternating, requireSameSlot: true},
	"BITOP":    {keyMode: topologyKeysBitOp, requireSameSlot: true},
	"RENAME":   {keyMode: topologyKeysFirstTwo, requireSameSlot: true},
	"RENAMENX": {keyMode: topologyKeysFirstTwo, requireSameSlot: true},
	"COPY":     {keyMode: topologyKeysFirstTwo, requireSameSlot: true},
	"LMOVE":    {keyMode: topologyKeysFirstTwo, requireSameSlot: true},
	"BLMOVE":   {keyMode: topologyKeysFirstTwo, requireSameSlot: true},
	"RPOPLPUSH": {
		keyMode: topologyKeysFirstTwo, requireSameSlot: true,
	},
	"BRPOPLPUSH": {
		keyMode: topologyKeysFirstTwo, requireSameSlot: true,
	},
	"SMOVE":          {keyMode: topologyKeysFirstTwo, requireSameSlot: true},
	"GEOSEARCHSTORE": {keyMode: topologyKeysFirstTwo, requireSameSlot: true},
	"BLMPOP":         {keyMode: topologyKeysBlockingCounted, requireSameSlot: true},
	"BZMPOP":         {keyMode: topologyKeysBlockingCounted, requireSameSlot: true},
	"BLPOP":          {keyMode: topologyKeysBlockingTrailing, requireSameSlot: true},
	"BRPOP":          {keyMode: topologyKeysBlockingTrailing, requireSameSlot: true},
	"BZPOPMIN":       {keyMode: topologyKeysBlockingTrailing, requireSameSlot: true},
	"BZPOPMAX":       {keyMode: topologyKeysBlockingTrailing, requireSameSlot: true},
	"SINTERCARD":     {keyMode: topologyKeysCountedRead, requireSameSlot: true},
	"ZINTERCARD":     {keyMode: topologyKeysCountedRead, requireSameSlot: true},
	"PFCOUNT":        {keyMode: topologyKeysAll, requireSameSlot: true},
	"SDIFF":          {keyMode: topologyKeysAll, requireSameSlot: true},
	"SINTER":         {keyMode: topologyKeysAll, requireSameSlot: true},
	"SUNION":         {keyMode: topologyKeysAll, requireSameSlot: true},
	"PFMERGE":        {keyMode: topologyKeysAll, requireSameSlot: true},
	"SDIFFSTORE":     {keyMode: topologyKeysAll, requireSameSlot: true},
	"SINTERSTORE":    {keyMode: topologyKeysAll, requireSameSlot: true},
	"SUNIONSTORE":    {keyMode: topologyKeysAll, requireSameSlot: true},
	"ZDIFFSTORE":     {keyMode: topologyKeysCountedStore, requireSameSlot: true},
	"ZINTERSTORE":    {keyMode: topologyKeysCountedStore, requireSameSlot: true},
	"ZUNIONSTORE":    {keyMode: topologyKeysCountedStore, requireSameSlot: true},
	"CMS.MERGE":      {keyMode: topologyKeysCountedStore, requireSameSlot: true},
	"TDIGEST.MERGE":  {keyMode: topologyKeysCountedStore, requireSameSlot: true},
	"XREAD":          {keyMode: topologyKeysStreams, requireSameSlot: true},
	"XREADGROUP":     {keyMode: topologyKeysStreams, requireSameSlot: true},

	"XGROUP": {keyMode: topologyKeysSubcommandThird},
	"XINFO":  {keyMode: topologyKeysHelpAwareThird},
	"OBJECT": {keyMode: topologyKeysHelpAwareThird},
	"MEMORY": {keyMode: topologyKeysMemoryUsage},

	"APPEND":                  {keyMode: topologyKeysFirst},
	"BF.ADD":                  {keyMode: topologyKeysFirst},
	"BF.CARD":                 {keyMode: topologyKeysFirst},
	"BF.EXISTS":               {keyMode: topologyKeysFirst},
	"BF.INFO":                 {keyMode: topologyKeysFirst},
	"BF.MADD":                 {keyMode: topologyKeysFirst},
	"BF.MEXISTS":              {keyMode: topologyKeysFirst},
	"BF.RESERVE":              {keyMode: topologyKeysFirst},
	"BITCOUNT":                {keyMode: topologyKeysFirst},
	"BITFIELD":                {keyMode: topologyKeysFirst},
	"BITPOS":                  {keyMode: topologyKeysFirst},
	"CAS":                     {keyMode: topologyKeysFirst},
	"CF.ADD":                  {keyMode: topologyKeysFirst},
	"CF.ADDNX":                {keyMode: topologyKeysFirst},
	"CF.COUNT":                {keyMode: topologyKeysFirst},
	"CF.DEL":                  {keyMode: topologyKeysFirst},
	"CF.EXISTS":               {keyMode: topologyKeysFirst},
	"CF.INFO":                 {keyMode: topologyKeysFirst},
	"CF.MEXISTS":              {keyMode: topologyKeysFirst},
	"CF.RESERVE":              {keyMode: topologyKeysFirst},
	"CMS.INCRBY":              {keyMode: topologyKeysFirst},
	"CMS.INFO":                {keyMode: topologyKeysFirst},
	"CMS.INITBYDIM":           {keyMode: topologyKeysFirst},
	"CMS.INITBYPROB":          {keyMode: topologyKeysFirst},
	"CMS.QUERY":               {keyMode: topologyKeysFirst},
	"DECR":                    {keyMode: topologyKeysFirst},
	"DECRBY":                  {keyMode: topologyKeysFirst},
	"EXPIRE":                  {keyMode: topologyKeysFirst},
	"EXPIREAT":                {keyMode: topologyKeysFirst},
	"EXPIRETIME":              {keyMode: topologyKeysFirst},
	"EXTEND":                  {keyMode: topologyKeysFirst},
	"FETCH_OR_COMPUTE":        {keyMode: topologyKeysFirst},
	"FETCH_OR_COMPUTE_ERROR":  {keyMode: topologyKeysFirst},
	"FETCH_OR_COMPUTE_RESULT": {keyMode: topologyKeysFirst},
	"FERRICSTORE.KEY_INFO":    {keyMode: topologyKeysFirst},
	"GET":                     {keyMode: topologyKeysFirst},
	"GETBIT":                  {keyMode: topologyKeysFirst},
	"GETDEL":                  {keyMode: topologyKeysFirst},
	"GETEX":                   {keyMode: topologyKeysFirst},
	"GETRANGE":                {keyMode: topologyKeysFirst},
	"GETSET":                  {keyMode: topologyKeysFirst},
	"GEOADD":                  {keyMode: topologyKeysFirst},
	"GEODIST":                 {keyMode: topologyKeysFirst},
	"GEOHASH":                 {keyMode: topologyKeysFirst},
	"GEOPOS":                  {keyMode: topologyKeysFirst},
	"GEOSEARCH":               {keyMode: topologyKeysFirst},
	"HDEL":                    {keyMode: topologyKeysFirst},
	"HEXISTS":                 {keyMode: topologyKeysFirst},
	"HGET":                    {keyMode: topologyKeysFirst},
	"HGETALL":                 {keyMode: topologyKeysFirst},
	"HGETDEL":                 {keyMode: topologyKeysFirst},
	"HGETEX":                  {keyMode: topologyKeysFirst},
	"HINCRBY":                 {keyMode: topologyKeysFirst},
	"HINCRBYFLOAT":            {keyMode: topologyKeysFirst},
	"HKEYS":                   {keyMode: topologyKeysFirst},
	"HLEN":                    {keyMode: topologyKeysFirst},
	"HMGET":                   {keyMode: topologyKeysFirst},
	"HMSET":                   {keyMode: topologyKeysFirst},
	"HSET":                    {keyMode: topologyKeysFirst},
	"HSETEX":                  {keyMode: topologyKeysFirst},
	"HSETNX":                  {keyMode: topologyKeysFirst},
	"HSTRLEN":                 {keyMode: topologyKeysFirst},
	"HTTL":                    {keyMode: topologyKeysFirst},
	"HEXPIRE":                 {keyMode: topologyKeysFirst},
	"HEXPIRETIME":             {keyMode: topologyKeysFirst},
	"HPERSIST":                {keyMode: topologyKeysFirst},
	"HPEXPIRE":                {keyMode: topologyKeysFirst},
	"HPEXPIRETIME":            {keyMode: topologyKeysFirst},
	"HPTTL":                   {keyMode: topologyKeysFirst},
	"HRANDFIELD":              {keyMode: topologyKeysFirst},
	"HSCAN":                   {keyMode: topologyKeysFirst},
	"HVALS":                   {keyMode: topologyKeysFirst},
	"INCR":                    {keyMode: topologyKeysFirst},
	"INCRBY":                  {keyMode: topologyKeysFirst},
	"INCRBYFLOAT":             {keyMode: topologyKeysFirst},
	"KEY_INFO":                {keyMode: topologyKeysFirst},
	"LINDEX":                  {keyMode: topologyKeysFirst},
	"LINSERT":                 {keyMode: topologyKeysFirst},
	"LLEN":                    {keyMode: topologyKeysFirst},
	"LOCK":                    {keyMode: topologyKeysFirst},
	"LPOP":                    {keyMode: topologyKeysFirst},
	"LPUSH":                   {keyMode: topologyKeysFirst},
	"LPUSHX":                  {keyMode: topologyKeysFirst},
	"LRANGE":                  {keyMode: topologyKeysFirst},
	"LREM":                    {keyMode: topologyKeysFirst},
	"LSET":                    {keyMode: topologyKeysFirst},
	"LTRIM":                   {keyMode: topologyKeysFirst},
	"LPOS":                    {keyMode: topologyKeysFirst},
	"PFADD":                   {keyMode: topologyKeysFirst},
	"PEXPIRE":                 {keyMode: topologyKeysFirst},
	"PEXPIREAT":               {keyMode: topologyKeysFirst},
	"PEXPIRETIME":             {keyMode: topologyKeysFirst},
	"PERSIST":                 {keyMode: topologyKeysFirst},
	"PSETEX":                  {keyMode: topologyKeysFirst},
	"PTTL":                    {keyMode: topologyKeysFirst},
	"RATELIMIT.ADD":           {keyMode: topologyKeysFirst},
	"RPOP":                    {keyMode: topologyKeysFirst},
	"RPUSH":                   {keyMode: topologyKeysFirst},
	"RPUSHX":                  {keyMode: topologyKeysFirst},
	"SADD":                    {keyMode: topologyKeysFirst},
	"SCARD":                   {keyMode: topologyKeysFirst},
	"SISMEMBER":               {keyMode: topologyKeysFirst},
	"SMEMBERS":                {keyMode: topologyKeysFirst},
	"SREM":                    {keyMode: topologyKeysFirst},
	"SET":                     {keyMode: topologyKeysFirst},
	"SETBIT":                  {keyMode: topologyKeysFirst},
	"SETEX":                   {keyMode: topologyKeysFirst},
	"SETNX":                   {keyMode: topologyKeysFirst},
	"SETRANGE":                {keyMode: topologyKeysFirst},
	"SMISMEMBER":              {keyMode: topologyKeysFirst},
	"SPOP":                    {keyMode: topologyKeysFirst},
	"SRANDMEMBER":             {keyMode: topologyKeysFirst},
	"SSCAN":                   {keyMode: topologyKeysFirst},
	"STRLEN":                  {keyMode: topologyKeysFirst},
	"TDIGEST.ADD":             {keyMode: topologyKeysFirst},
	"TDIGEST.BYRANK":          {keyMode: topologyKeysFirst},
	"TDIGEST.BYREVRANK":       {keyMode: topologyKeysFirst},
	"TDIGEST.CDF":             {keyMode: topologyKeysFirst},
	"TDIGEST.CREATE":          {keyMode: topologyKeysFirst},
	"TDIGEST.INFO":            {keyMode: topologyKeysFirst},
	"TDIGEST.MAX":             {keyMode: topologyKeysFirst},
	"TDIGEST.MIN":             {keyMode: topologyKeysFirst},
	"TDIGEST.QUANTILE":        {keyMode: topologyKeysFirst},
	"TDIGEST.RANK":            {keyMode: topologyKeysFirst},
	"TDIGEST.RESET":           {keyMode: topologyKeysFirst},
	"TDIGEST.REVRANK":         {keyMode: topologyKeysFirst},
	"TDIGEST.TRIMMED_MEAN":    {keyMode: topologyKeysFirst},
	"TOPK.ADD":                {keyMode: topologyKeysFirst},
	"TOPK.COUNT":              {keyMode: topologyKeysFirst},
	"TOPK.INCRBY":             {keyMode: topologyKeysFirst},
	"TOPK.INFO":               {keyMode: topologyKeysFirst},
	"TOPK.LIST":               {keyMode: topologyKeysFirst},
	"TOPK.QUERY":              {keyMode: topologyKeysFirst},
	"TOPK.RESERVE":            {keyMode: topologyKeysFirst},
	"TTL":                     {keyMode: topologyKeysFirst},
	"TYPE":                    {keyMode: topologyKeysFirst},
	"UNLOCK":                  {keyMode: topologyKeysFirst},
	"XADD":                    {keyMode: topologyKeysFirst},
	"XACK":                    {keyMode: topologyKeysFirst},
	"XDEL":                    {keyMode: topologyKeysFirst},
	"XLEN":                    {keyMode: topologyKeysFirst},
	"XRANGE":                  {keyMode: topologyKeysFirst},
	"XREVRANGE":               {keyMode: topologyKeysFirst},
	"XTRIM":                   {keyMode: topologyKeysFirst},
	"ZADD":                    {keyMode: topologyKeysFirst},
	"ZCARD":                   {keyMode: topologyKeysFirst},
	"ZCOUNT":                  {keyMode: topologyKeysFirst},
	"ZINCRBY":                 {keyMode: topologyKeysFirst},
	"ZRANGE":                  {keyMode: topologyKeysFirst},
	"ZRANGEBYSCORE":           {keyMode: topologyKeysFirst},
	"ZRANDMEMBER":             {keyMode: topologyKeysFirst},
	"ZREM":                    {keyMode: topologyKeysFirst},
	"ZREVRANGE":               {keyMode: topologyKeysFirst},
	"ZREVRANGEBYSCORE":        {keyMode: topologyKeysFirst},
	"ZMSCORE":                 {keyMode: topologyKeysFirst},
	"ZPOPMAX":                 {keyMode: topologyKeysFirst},
	"ZPOPMIN":                 {keyMode: topologyKeysFirst},
	"ZRANK":                   {keyMode: topologyKeysFirst},
	"ZSCAN":                   {keyMode: topologyKeysFirst},
	"ZSCORE":                  {keyMode: topologyKeysFirst},
	"ZREVRANK":                {keyMode: topologyKeysFirst},
}

func topologyPolicyKeys(name string, args []any) ([]any, topologyCommandPolicy, bool) {
	policy, ok := topologyCommandPolicies[name]
	if !ok {
		return nil, topologyCommandPolicy{}, false
	}
	keys, ok := topologyKeysForMode(policy.keyMode, args)
	if !ok {
		return nil, policy, false
	}
	return keys, policy, true
}

func topologyKeysForMode(mode topologyKeyMode, args []any) ([]any, bool) {
	switch mode {
	case topologyKeysFirst:
		if len(args) > 1 {
			return args[1:2], true
		}
	case topologyKeysAll:
		if len(args) > 1 {
			return args[1:], true
		}
	case topologyKeysAlternating:
		if len(args) >= 3 && (len(args)-1)%2 == 0 {
			keys := make([]any, 0, (len(args)-1)/2)
			for index := 1; index < len(args); index += 2 {
				keys = append(keys, args[index])
			}
			return keys, true
		}
	case topologyKeysFirstTwo:
		if len(args) >= 3 {
			return args[1:3], true
		}
	case topologyKeysBitOp:
		if len(args) >= 4 {
			return args[2:], true
		}
	case topologyKeysCountedRead:
		return countedReadKeys(args)
	case topologyKeysCountedStore:
		return countedStoreKeys(args)
	case topologyKeysBlockingCounted:
		if len(args) >= 4 {
			count, ok := boundedRoutingCount(args[2], len(args)-3, false)
			if ok {
				return args[3 : 3+count], true
			}
		}
	case topologyKeysBlockingTrailing:
		if len(args) >= 3 {
			return args[1 : len(args)-1], true
		}
	case topologyKeysStreams:
		return streamReadKeys(args)
	case topologyKeysSubcommandThird:
		if len(args) > 2 {
			return args[2:3], true
		}
	case topologyKeysHelpAwareThird:
		if len(args) > 2 && commandPart(args[1]) != "HELP" {
			return args[2:3], true
		}
	case topologyKeysMemoryUsage:
		if len(args) > 2 && commandPart(args[1]) == "USAGE" {
			return args[2:3], true
		}
	}
	return nil, false
}

func streamReadKeys(args []any) ([]any, bool) {
	if len(args) < 2 {
		return nil, false
	}
	name := commandName(args)
	index := 1
	if name == "XREADGROUP" {
		if len(args) < 5 || commandPart(args[1]) != "GROUP" {
			return nil, false
		}
		// GROUP's group and consumer operands are arbitrary strings and may
		// themselves be named STREAMS. Skip them before parsing options.
		index = 4
	}
	for index < len(args) {
		switch commandPart(args[index]) {
		case "STREAMS":
			streamArgs := args[index+1:]
			if len(streamArgs) == 0 || len(streamArgs)%2 != 0 {
				return nil, false
			}
			return streamArgs[:len(streamArgs)/2], true
		case "COUNT", "BLOCK":
			if index+1 >= len(args) {
				return nil, false
			}
			index += 2
		case "NOACK":
			if name != "XREADGROUP" {
				return nil, false
			}
			index++
		default:
			return nil, false
		}
	}
	return nil, false
}
