package deliverymq_test

import (
	"context"
	"testing"
	"time"

	"github.com/hookdeck/outpost/internal/backoff"
	"github.com/hookdeck/outpost/internal/deliverymq"
	"github.com/hookdeck/outpost/internal/idempotence"
	"github.com/hookdeck/outpost/internal/idgen"
	"github.com/hookdeck/outpost/internal/models"
	"github.com/hookdeck/outpost/internal/util/testutil"
	"github.com/stretchr/testify/require"
)

type verdictAwaiter models.AcceptanceVerdict

func (v verdictAwaiter) Await(context.Context, models.Acceptance) (models.AcceptanceVerdict, error) {
	return models.AcceptanceVerdict(v), nil
}

// What the delivery worker does with a gated publish's task for each acceptance verdict
// (internal/redis owns the verdicts against a real Redis). A task whose record is past
// its retention is delivered: dropping it could lose an accepted event for good.
func TestMessageHandler_GatedTaskFollowsItsAcceptance(t *testing.T) {
	cases := []struct {
		name      string
		awaiter   deliverymq.AcceptanceAwaiter
		delivered bool
	}{
		{"accepted", verdictAwaiter(models.Accepted), true},
		{"never accepted", verdictAwaiter(models.NeverAccepted), false},
		{"record past its retention", verdictAwaiter(models.RetentionPassed), true},
		{"no acceptance reader configured", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tenant := models.Tenant{ID: idgen.String()}
			destination := testutil.DestinationFactory.Any(testutil.DestinationFactory.WithTenantID(tenant.ID))
			event := testutil.EventFactory.Any(testutil.EventFactory.WithTenantID(tenant.ID), testutil.EventFactory.WithDestinationID(destination.ID))
			publisher := newMockPublisher([]error{nil})
			var opts []deliverymq.MessageHandlerOption
			if tc.awaiter != nil {
				opts = append(opts, deliverymq.WithAcceptance(tc.awaiter))
			}
			handler := deliverymq.NewMessageHandler(
				testutil.CreateTestLogger(t),
				newMockLogPublisher(nil),
				&mockDestinationGetter{dest: &destination},
				publisher,
				testutil.NewMockEventTracer(nil),
				newMockRetryScheduler(),
				&backoff.ConstantBackoff{Interval: time.Second},
				10,
				idempotence.New(testutil.CreateTestRedisClient(t), idempotence.WithSuccessfulTTL(time.Hour)),
				opts...,
			)
			task := models.NewDeliveryTask(event, destination.ID)
			task.Acceptance = &models.Acceptance{Key: "mbgate:accepted:t", Fence: 1, Expires: 2}
			mock, msg := newDeliveryMockMessage(task)
			require.NoError(t, handler.Handle(context.Background(), msg))
			require.True(t, mock.acked, "the task is acknowledged either way")
			require.False(t, mock.nacked)
			if tc.delivered {
				require.Equal(t, 1, publisher.Current(), "the task was not delivered")
			} else {
				require.Equal(t, 0, publisher.Current(), "the task was delivered")
			}
		})
	}
}
