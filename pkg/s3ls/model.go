package s3ls

import (
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// Item represents an S3 Item.
type Item struct {
	Bucket   string
	Object   types.Object
	Metadata *s3.HeadObjectOutput
}

// Entry represents all of the properties an individual S3 Object.
type Entry struct {
	Key                       string            `json:"key"`
	Bucket                    string            `json:"bucket"`
	Size                      int64             `json:"size"`
	ETagObject                string            `json:"etagObject,omitempty"`
	LastModifiedObject        *time.Time        `json:"lastModifiedObject,omitempty"`
	StorageClassObject        string            `json:"storageClassObject,omitempty"`
	OwnerID                   string            `json:"ownerId,omitempty"`
	OwnerName                 string            `json:"ownerName,omitempty"`
	AcceptRanges              string            `json:"acceptRanges,omitempty"`
	CacheControl              string            `json:"cacheControl,omitempty"`
	ContentDisposition        string            `json:"contentDisposition,omitempty"`
	ContentEncoding           string            `json:"contentEncoding,omitempty"`
	ContentLanguage           string            `json:"contentLanguage,omitempty"`
	ContentLength             int64             `json:"contentLength,omitempty"`
	ContentType               string            `json:"contentType,omitempty"`
	ETag                      string            `json:"etag,omitempty"`
	Expiration                string            `json:"expiration,omitempty"`
	Expires                   *time.Time        `json:"expires,omitempty"`
	LastModified              *time.Time        `json:"lastModified,omitempty"`
	Metadata                  map[string]string `json:"metadata,omitempty"`
	MissingMeta               int32             `json:"missingMeta,omitempty"`
	ObjectLockLegalHoldStatus string            `json:"objectLockLegalHoldStatus,omitempty"`
	ObjectLockRetainUntilDate *time.Time        `json:"objectLockRetainUntilDate,omitempty"`
	ObjectLockMode            string            `json:"objectLockMode,omitempty"`
	PartsCount                int32             `json:"partsCount,omitempty"`
	ReplicationStatus         string            `json:"replicationStatus,omitempty"`
	RequestCharged            string            `json:"requestCharged,omitempty"`
	Restore                   string            `json:"restore,omitempty"`
	SSECustomerAlgorithm      string            `json:"sseCustomerAlgorithm,omitempty"`
	SSECustomerKeyMD5         string            `json:"sseCustomerKeyMD5,omitempty"`
	SSEKMSKeyID               string            `json:"sseKMSKeyId,omitempty"`
	ServerSideEncryption      string            `json:"serverSideEncryption,omitempty"`
	StorageClass              string            `json:"storageClass,omitempty"`
	VersionID                 string            `json:"versionId,omitempty"`
	WebsiteRedirectLocation   string            `json:"websiteRedirectLocation,omitempty"`
	DeleteMarker              bool              `json:"deleteMarker,omitempty"`
	Invalid                   bool              `json:"invalid,omitempty"` // true if head data cannot be retrieved
	Touched                   bool              `json:"-"`
}
