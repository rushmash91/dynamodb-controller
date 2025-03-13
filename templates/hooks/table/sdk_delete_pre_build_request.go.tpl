	if isTableDeleting(r) {
		return nil, requeueWaitWhileDeleting
	}
	if isTableUpdating(r) {
		return nil, requeueWaitWhileUpdating
	}

	// If there are replicas, we need to remove them before deleting the table
	if r.ko.Spec.Replicas != nil && len(r.ko.Spec.Replicas) > 0 {
		desired := &resource{
			ko: r.ko.DeepCopy(),
		}
		desired.ko.Spec.Replicas = nil

		err := rm.syncReplicaUpdates(ctx, r, desired)
		if err != nil {
			return nil, err
		}
	}

	r.ko.Spec.Replicas = nil
