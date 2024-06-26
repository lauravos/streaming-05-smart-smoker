# streaming-05-smart-smoker

## Laura Gagnon-Vos
#### 06/07/2024

## The Problem / Challenge To Solve
Please read about the Smart Smoker system here: Smart Smoker

Access the smoker data file here Download smoker data file here.

We want to stream information from a smart smoker. Read one value every half minute. (sleep_secs = 30)

smoker-temps.csv has 4 columns:

[0] Time = Date-time stamp for the sensor reading

[1] Channel1 = Smoker Temp --> send to message queue "01-smoker"

[2] Channel2 = Food A Temp --> send to message queue "02-food-A"

[3] Channel3 = Food B Temp --> send to message queue "03-food-B"


## Significant Events
We want know if:

The smoker temperature decreases by more than 15 degrees F in 2.5 minutes (smoker alert!)

Any food temperature changes less than 1 degree F in 10 minutes (food stall!)

### Time Windows

Smoker time window is 2.5 minutes

Food time window is 10 minutes

### Deque Max Length

At one reading every 1/2 minute, the smoker deque max length is 5 (2.5 min * 1 reading/0.5 min)

At one reading every 1/2 minute, the food deque max length is 20 (10 min * 1 reading/0.5 min) 

### Condition To monitor

If smoker temp decreases by 15 F or more in 2.5 min (or 5 readings)  --> smoker alert!

If food temp change in temp is 1 F or less in 10 min (or 20 readings)  --> food stall alert!

## Requirements

RabbitMQ server running
pika installed in your active environment
RabbitMQ Admin

See http://localhost:15672/Links to an external site.
General Design Questions

How many producers processes do you need to read the temperatures:
How many queues do we use: 
How many listening callback functions do we need (Hint: one per queue): 
If that is all you need to get started, you can begin the project now. Apply everything you've learned previously. 


## Task 1. Create a Place to Work
1. In GitHub, create a new repo for your project - name it streaming-05-smart-smoker
2. Add a README.md during the creation process. (If not, you can always add it later.)
3. Clone your repo down to your machine. 
4. In VS Code, add a .gitignore (use one from an earlier module), start working on the README.md. Create it if you didn't earlier.
5. Add the csv data file to your repo. 
6. Create a file for your bbq producer.


## Task 2. Design and Implement Your Producer
1. Implement your bbq producer. More detailed help provided in links below. 
2. Use the logic, approach, and structure from Module 4, version 2 and version 3.
3. These provide a current and solid foundation for streaming analytics - modifying them to serve your purpose IS part of the assignment.
4. Do not start from scratch - do not search for code - do not use a notebook.
5. Use comments in the code and repo to explain your work. 
6. Use docstring comments and add your name and date to your README and your code files. 
7. Explain your project in the README. Include prerequisites and how to run your code. 
8. Document your project works - display screenshots of your console and maybe the RabbitMQ console. 
9. If you only have a producer, you won't have a consumer showing messages yet, so you'll need to be creative. We'll build the consumers next.

## Task 3. Design and Implement Each Consumer
1. Design and implement each bbq consumer. You could have one. You could have 3.  More detailed help provided in links below. 
2. Use the logic, approach, and structure from prior modules (use the recommended versions).
3. Modifying them to serve your purpose IS part of the assignment.
4. Do not start from scratch - do not search for code - do not use a notebook.
5. Use comments in the code and repo to explain your work. 
6. Use docstring comments and add your name and date to your README and your code files. 

## Required Approach
Use your Module 4 projects (Version 2 and Version 3) as examples.

Remember: No prior coding experience is required to take this course. Rely heavily on the working examples from earlier modules. 

The more similar your code looks to the examples - the more credit earned.

Vastly different approaches can be expected to earn less credit not more.

This project should clearly build on skills and code we've already mastered. If not, let me know and more help will be provided. 

The primary difference should be going from 1 to 3 queue_names and from 1 to 3 callbacks. 

Part of the challenge is to implement analytics using the tools and approach provided (don't significantly refactor the codebase during your first week of work!) 

AFTER earning credit for the assignment, THEN create and share additional custom projects. 


## Screenshots:
1. Producer (getting the temperature readings)
![Screenshot showing producer](./image_producer.png)
2. Smoker monitor
![Screenshot showing producer](./image_smoker.png)
3. Food A monitor
![Screenshot showing producer](./image_foodA.png)
4. Food B monitor
![Screenshot showing producer](./image_foodB.png)
