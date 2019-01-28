from click_web import create_click_web_app
import example_command

app = create_click_web_app(example_command, example_command.cli)
