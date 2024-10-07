from data_science_pipeline.utils.misc import iter_batches


class TestIterBatches:
    def test_should_batch_list(self):
        assert list(iter_batches(
            [0, 1, 2, 3, 4],
            2
        )) == [[0, 1], [2, 3], [4]]

    def test_should_batch_iterable(self):
        assert list(iter_batches(
            iter([0, 1, 2, 3, 4]),
            2
        )) == [[0, 1], [2, 3], [4]]
