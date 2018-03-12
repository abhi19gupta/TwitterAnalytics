print("From inside the post-proc")
print(result)

3.2.1.5 Functional requirement 1.5
ID: FR5
TITLE: Posting a journey
DESC: A User can post a "journey"(an entity of our system) on the app. The jouney will include the following information:
The tentative date/time of journey
The destination
The preferred means of transport
Preffered number of people you want to travel with
DEP:

3.2.1.6 Functional requirement 1.6
ID: FR6
TITLE: Searching for journey
DESC: The user can search for a journey, again by providing the details as mentioned in 3.2.1.5. Matching based on some criteria
will be done. The matching journeys will be shown to the user in a list format in the app with the percentage match.

3.2.1.7 Functional requirement 1.7
ID: FR7
TITLE: Setting notification for a journey
DESC: The user can choose to provide details for the jounney and then subsribe to notifications if someone posts an overlapping
journey by specifying the amount of overlap. If some other user posts a journey with overlap greater than the specified, the user
will get notification in the app.

3.2.1.8 Functional requirement 1.8
ID: FR8
TITLE: Contacting the journey poster
DESC: The user can call OR contact through facebook, email the journey poster.

3.2.1.9 Functional requirement 1.9
ID: FR9
TITLE: Adding a user to the journey
DESC: The poster of the journey can add some other user to the journey. The users included in an open journey will be shown
to the other users who search for a journey.

3.2.1.10 Functional requirement 1.10
ID: FR10
TITLE: Marking the journey complete
DESC: If the number of users included in the journey has reached the optimal as specified by the poster, the journey will be automatically closed.
The user can choose to close the journey manually also. A closed journey will not be shown to other users.

3.2.1.11 Functional requirement 1.11
ID: FR11
TITLE: Withdraw from the journey
DESC: The users added by the journey poster can decide to withdraw from the journey. A notification of such withdrawl will be sent to all the users included in the journey.
The number of withdrawls will be shown in the profile of a user.



3.2.3.1 Functional requirement 1.12
ID: FR12
TITLE: Posting a trip intent
DESC: A User should be able to post the intent of a trip on the app. This will include the following information:
Preferred destination for the trip
Preferred date/time of the trip
Budget range estimate - transportation/food, etc
DEP:

3.2.3.2 Functional requirement 1.13
ID: FR12
TITLE: Notification for similar trip intents
DESC: A User will get notification if some other user posts a trip intent which have overlap with the intent posted by the user. In the notification, the user should be shown 
the overlap in an intuitive manner.
DEP:

3.2.3.2 Functional requirement 1.13
ID: FR12
TITLE: Formation of trip planning group
DESC: Based on the trip intents posted by the users, trip planning group will be formed. The user can decide to form a group automatically where the other 
users with overlapping tripo intents will be added or can manually add other users also. The contact details of every member of the trip planning group will be shared
will every other member of the group.
DEP:

3.2.3.2 Functional requirement 1.13
ID: FR12
TITLE: Election of coordinator
DESC: The members of the group will elect the coordinator in a secret polling. In case of a tie, a random coin toss will be done. The coordinator will be the point of contact for the trip in out app.
DEP: 

3.2.3.2 Functional requirement 1.13
ID: FR12
TITLE: Posting of trip
DESC: Please observe the difference between a "trip intent" and a "trip". After the finalizing in the trip planning group, the coordinator may decide to post the trip on the app if they have room for more poeple to join into the trip. The trip will include details like:
Location of the trip
Exact timeline of the trip - start/end/travel times
Exact cost of the trip with details 
Further, the coordinator can decide to reveal the group members when posting the trip or may decide to keep it as secret due to privacy reasons.
DEP:

3.2.3.2 Functional requirement 1.13
ID: FR12
TITLE: Searching for trips
DESC: All the trips posted will be publicly available to all the users. Further a user can search for trips based on filters, like the location, budget etc.
DEP:  

3.2.3.2 Functional requirement 1.13
ID: FR12
TITLE: Request for joining a trip
DESC: A user make a request to join a trip. The user can't negotiate the details of the trip and will have to accept the trip as is while making the request.The details of the request maker will be shared with the trip coordinator. This details will include the past withdrawls made by the user.  
DEP:  

3.2.3.2 Functional requirement 1.13
ID: FR12
TITLE: Approval/rejection of the request by the trip coordinator
DESC: The request will appear as a notification to the trip coordinator. The coordinator will be provided with the detials of the requestor. He can make a descision to accept/reject
the request after contact the person.
DEP:

3.2.1.10 Functional requirement 1.10
ID: FR10
TITLE: Marking the trip complete
DESC: After completion of the trip, the coordinator will be required to mark the trip as compeleted so that it is removed from the seach results to other users.
DEP:  

3.2.1.11 Functional requirement 1.11
ID: FR11
TITLE: Withdraw from the trip
DESC: Between joining the trip and completion of the trip, a trip member can decide to withdraw from the trip. A notification of such withdrawl will be sent to all the trip members.
The number of withdrawls will be shown in the profile of a user.
DEP:  

3.2.1.11 Functional requirement 1.11
ID: FR11
TITLE: Providing Rating/Reviews about the trip
DESC: After completion of the trip, the coordinator can get "cordination points", after fillling a small survey where he/she will be required to rate and provide comments about specific aspects of the trip.
This is necessary to get data for analysis.
DEP:  

3.2.1.11 Functional requirement 1.11
ID: FR11
TITLE: See the Rating/Reviews about trip destinations, hotels/lodges
DESC: The ratings/reviews provided by the trip coordinator will be analysed and compiled so that a user can search about entities and can see the rating/reviews about it.
The destination reviews will be shown on a map.
DEP:  
