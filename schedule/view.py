from flask import Blueprint, render_template, request, redirect, url_for

from src.course.course import Course
from src.combinator.combinator import Combinator

schedule_blueprint = Blueprint('schedule', __name__)


@schedule_blueprint.route('/')
def index(schedule_obj):
    return render_template('schedule.jinja2', schedulelist=schedule_obj)
