from flask import Blueprint, render_template, request
from src.course.course import Course
from src.combinator.combinator import Combinator
course_blueprint = Blueprint('course', __name__)


@course_blueprint.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'GET':
        courses = Course.get_all_courses()
        return render_template('courses.jinja2', courses=courses)

    elif request.method == 'POST':
        courses = request.form['courses'].split(',')
        # TODO - This now requires term ID set
        schedule = Combinator('201808')
        if len(courses) > 1:
            schedule.add_many([courses[i:i + 2] for i in range(0, len(courses), 2)])
        schedule.do_all()
        return render_template('schedule.jinja2', schedulelist=schedule)
