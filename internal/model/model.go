package model

import "time"

type KOLRanking struct {
	Username                string
	DisplayName             string
	ProfileURL              string
	AvatarURL               string
	Bio                     string
	Location                string
	CreateTime              *time.Time
	GlobalRank              *int
	CNRank                  *int
	ENRank                  *int
	Classification          string
	IsCN                    bool
	FollowersCount          int64
	FollowingCount          int64
	ListedCount             int64
	TweetsCount             int64
	GlobalKOLFollowersCount int
	CNKOLFollowersCount     int
	TopKOLFollowersCount    int
	DiscoveryDepth          int
	DiscoveredByCount       int
	FirstDiscoveredAt       time.Time
	LastDiscoveredAt        time.Time
	LastFetchedAt           *time.Time
}

type CrawlSeen struct {
	Username        string
	DiscoveryDepth  int
	IsEnqueued      bool
	IsFetched       bool
	FetchStatus     string
	AttemptCount    int
	RateLimitCount  int
	LastAttemptAt   *time.Time
	LastSuccessAt   *time.Time
	NextRetryAt     *time.Time
	LastError       string
	FirstEnqueuedAt time.Time
	LastEnqueuedAt  time.Time
}

type PendingAccount struct {
	Username       string
	DiscoveryDepth int
}

type UserInfoResponse struct {
	Data *UserInfo `json:"data"`
	Err  string    `json:"err"`
}

type UserInfo struct {
	Name       string     `json:"name"`
	Desc       string     `json:"desc"`
	CreateTime *time.Time `json:"create_time"`
	Feature    struct {
		Rank struct {
			KOLRank *int `json:"kolRank"`
		} `json:"rank"`
		KOLFollowers KOLFollowers `json:"kol_followers"`
	} `json:"feature"`
	AI struct {
		Classification string `json:"classification"`
		IsCN           bool   `json:"is_cn"`
	} `json:"ai"`
	Profile struct {
		Avatar          string `json:"avatar"`
		ProfileImageURL string `json:"profile_image_url"`
		Description     string `json:"description"`
		Location        string `json:"location"`
		FollowersCount  int    `json:"followers_count"`
		FollowingCount  int    `json:"following_count"`
		ListedCount     int    `json:"listed_count"`
		TweetsCount     int    `json:"tweets_count"`
	} `json:"profile"`
	Avatar string `json:"avatar"`
}

type KOLFollowers struct {
	GlobalKOLFollowers      []Follower `json:"globalKolFollowers"`
	GlobalKOLFollowersCount int        `json:"globalKolFollowersCount"`
	CNKOLFollowers          []Follower `json:"cnKolFollowers"`
	CNKOLFollowersCount     int        `json:"cnKolFollowersCount"`
	TopKOLFollowers         []Follower `json:"topKolFollowers"`
	TopKOLFollowersCount    int        `json:"topKolFollowersCount"`
}

type Follower struct {
	Avatar   string `json:"avatar"`
	Name     string `json:"name"`
	Username string `json:"username"`
}

func (u *UserInfo) FollowersByBucket() map[string][]Follower {
	return map[string][]Follower{
		"global": u.Feature.KOLFollowers.GlobalKOLFollowers,
		"cn":     u.Feature.KOLFollowers.CNKOLFollowers,
		"top100": u.Feature.KOLFollowers.TopKOLFollowers,
	}
}

type TopRankingList struct {
	Data []TopRankingRow `json:"data"`
}

type TopRankingRow struct {
	ID           string     `json:"id"`
	Name         string     `json:"name"`
	Username     string     `json:"username"`
	UsernameRaw  string     `json:"username_raw"`
	Rank         *int       `json:"rank"`
	RankChange   *int       `json:"rank_change"`
	RankUpdateAt *time.Time `json:"rank_update_at"`
	CreateTime   *time.Time `json:"create_time"`
	AI           struct {
		Classification string `json:"classification"`
		IsCN           bool   `json:"is_cn"`
	} `json:"ai"`
	Profile struct {
		Description     string `json:"description"`
		ProfileImageURL string `json:"profile_image_url"`
		Name            string `json:"name"`
		Username        string `json:"username"`
		UsernameRaw     string `json:"username_raw"`
		Location        string `json:"location"`
		FollowersCount  int64  `json:"followers_count"`
		FollowingCount  int64  `json:"following_count"`
		ListedCount     int64  `json:"listed_count"`
		TweetsCount     int64  `json:"tweets_count"`
	} `json:"profile"`
}

type ImportedRankKind string

const (
	ImportedRankKindUnknown ImportedRankKind = "unknown"
	ImportedRankKindGlobal  ImportedRankKind = "global"
	ImportedRankKindCN      ImportedRankKind = "cn"
	ImportedRankKindEN      ImportedRankKind = "en"
)
