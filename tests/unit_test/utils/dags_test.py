from data_science_pipeline.utils.dags import (
    get_default_notebook_task_id
)


class TestGetDefaultNotebookTaskId:
    def test_should_return_passed_in_value_without_special_characters(self):
        assert get_default_notebook_task_id('test123') == 'test123'

    def test_should_strip_ext(self):
        assert get_default_notebook_task_id('test123.ext') == 'test123'

    def test_should_strip_dirname(self):
        assert get_default_notebook_task_id('/path/to/test123.ext') == 'test123'

    def test_should_replace_special_characters_with_underscore(self):
        assert get_default_notebook_task_id('test!123.ext') == 'test_123'
