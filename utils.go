package drain

import "time"

// nowFunc is a variable that can be overridden for testing
var nowFunc = time.Now
