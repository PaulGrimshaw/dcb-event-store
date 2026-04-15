import inquirer from "inquirer"
import { Api } from "./api/Api.js"

export const startCli = async (api: Api) => {
    log("Program started succesfully")

    while (true) {
        const choices = await inquirer.prompt({
            type: "list",
            name: "action",
            message: "What do you want to do?",
            choices: [
                "Register course",
                "Register student",
                "Update course capacity",
                "Update course title",
                "Subscribe student to course",
                "Unsubscribe student from course",
                "Reset database",
                "Exit"
            ]
        })

        try {
            switch (choices.action) {
                case "Register course": {
                    const course = await inquirer.prompt([
                        { name: "id", message: "Course ID:", type: "input" },
                        { name: "title", message: "Course title:", type: "input" },
                        { name: "capacity", message: "Course capacity:", type: "number" }
                    ])
                    await api.registerCourse(course)
                    log(`Course with id ${course.id} and capacity ${course.capacity} registered`)
                    break
                }

                case "Update course title": {
                    const { courseId, newTitle } = await inquirer.prompt([
                        { name: "courseId", message: "Course ID:", type: "input" },
                        { name: "newTitle", message: "Course title:", type: "input" }
                    ])
                    await api.updateCourseTitle({ courseId, newTitle })
                    break
                }

                case "Update course capacity": {
                    const { courseId, newCapacity } = await inquirer.prompt([
                        { name: "courseId", message: "Course ID:", type: "input" },
                        { name: "newCapacity", message: "Course capacity:", type: "number" }
                    ])
                    await api.updateCourseCapacity({ courseId, newCapacity })
                    break
                }

                case "Register student": {
                    const student = await inquirer.prompt([
                        { name: "id", message: "Student ID:", type: "input" },
                        { name: "name", message: "Student name:", type: "input" }
                    ])
                    await api.registerStudent(student)
                    log(`Student with id ${student.id} and name ${student.name} registered`)
                    break
                }

                case "Subscribe student to course": {
                    const { courseId, studentId } = await inquirer.prompt([
                        { name: "studentId", message: "Student ID:", type: "input" },
                        { name: "courseId", message: "Course ID:", type: "input" }
                    ])
                    await api.subscribeStudentToCourse({ courseId, studentId })
                    log(`Student ${studentId} subscribed to course ${courseId}`)
                    break
                }

                case "Unsubscribe student from course": {
                    const { courseId, studentId } = await inquirer.prompt([
                        { name: "studentId", message: "Student ID:", type: "input" },
                        { name: "courseId", message: "Course ID:", type: "input" }
                    ])
                    await api.unsubscribeStudentFromCourse({ courseId, studentId })
                    log(`Student ${studentId} unsubscribed from course ${courseId}`)
                    break
                }

                case "Exit":
                    process.exit(0)
                    break

                default:
                    log("No valid action selected.")
                    break
            }
        } catch (err) {
            if (process.env.DEBUG) throw err
            log(<Error>err)
        }
    }
}

const log = (message: string | object | Error | undefined) => {
    console.log(`______________________________________________________`)
    if (message instanceof Error) {
        console.log(`\n\x1b[31m${message?.message ?? message}\x1b[0m`)
    } else {
        console.log(`\n${typeof message === "object" ? JSON.stringify(message, null, 2) : message}`)
    }
    console.log(`______________________________________________________\n`)
}
