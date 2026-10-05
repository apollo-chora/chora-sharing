package pg
import (
	"github.com/apollo-chora/chora-sharing/internal/domain/post"
)
// compile-time assertion
var _ post.PostRepo = (*PostRepository)(nil)
