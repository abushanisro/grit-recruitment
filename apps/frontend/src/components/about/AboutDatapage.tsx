import { Box, ScrollArea, Text } from "@mantine/core";
import { useMediaQuery } from "@mantine/hooks";
import React from "react";

const AboutDatapage = () => {
  const isMobile = useMediaQuery("(max-width: 426px)");
  const CustomHeadingText = ({ label }: { label: string }) => {
    return (
      <Text
        mt={{ base: 14, md: 24, lg: 20, xl: 20 }}
        c="#FFF"
        fw={700}
        fz={{ base: 18, xs: 20, sm: 24, md: 24, lg: 24, xl: 24 }}
      >
        {label}
      </Text>
    );
  };

  const CustomContentText = ({ label }: { label: string }) => {
    return (
      <Text
        style={{ textAlign: "justify" }}
        mt={{ base: 14, lg: 20, xl: 20 }}
        fz={{ base: 14, xs: 16, sm: 18, md: 18, lg: 18, xl: 18 }}
        fw={400}
      >
        {label}
      </Text>
    );
  };

  return (
    <Box>
      <Box
        bg={"primaryGrey.1"}
        style={{
          borderRadius: 10,
          height: isMobile ? 80 : 120,
          width: "100%",
        }}
      >
        <Text
          pt={isMobile ? 30 : 60}
          fz={{ base: 20, xs: 24, sm: 26, md: 33, lg: 33, xl: 33 }}
          fw={700}
          style={{ textAlign: "center" }}
        >
          About
        </Text>
      </Box>
      <ScrollArea h={800}>
        <Box
          px={{ base: 20, xs: 50, sm: 50, xl: 70 }}
          py={{ base: 20, xs: 50, sm: 50, xl: 60 }}
        >
          <Box>
            <CustomHeadingText label={"Our Story"} />
            <CustomContentText
              label={`Finding a job is hard. But so is recruiting candidates for a job. 

`}
            />
            <CustomContentText
              label={`And when it comes to recruiting for a sales job, we found it's even harder.
`}
            />
            <CustomContentText
              label={`Sales people are super important for almost every company. 
 `}
            />
            <CustomContentText
              label={`Yet, people aren’t even considering a sales career. `}
            />

            <CustomContentText
              label={` 
               Most hardly know anything about sales, and some even consider it taboo. 
               `}
            />

            <CustomContentText
              label={`In reality, sales is a transformational, enriching and rewarding experience, even to pursue as a short stint if not as a career. 
 `}
            />
            <CustomContentText
              label={`So here’s a role, which is on one hand high in demand, and the other, doesn’t even have much competition, all while being so enriching. 

`}
            />
            <CustomContentText
              label={`To bridge this gap, all you need is a platform to talk about sales careers, and give it the deserved hype. 

`}
            />

            <CustomContentText
              label={`Enter, Sales Jobverse. 
`}
            />

            <CustomContentText
              label={`It’s a sincere effort to celebrate sales and give it the hype it deserves. 

`}
            />

            <CustomContentText
              label={`We have had a tough time recruiting for sales in our other businesses, and seen our friends share the same struggle. 

`}
            />

            <CustomContentText
              label={`If we can make some people see the light, and elevate the candidate pool for sales; candidates, companies and recruiters, are all going to be a lot happier.



`}
            />
            <CustomContentText
              label={`And eventually, we hope to play a part in elevating sales standards and ethics. 



`}
            />
            <CustomContentText
              label={`Ultimately, sales is powerful, and with great power comes great responsibility.  



`}
            />

            <CustomHeadingText
              label={"Can you imagine yourself doing sales?"}
            />

            <CustomContentText
              label={`Well, you don’t have to imagine. You have been doing sales all your life.
`}
            />

            <CustomContentText
              label={`From asking for an extra toy as a child, to negotiating play time, finalising travel plans, deciding your career, and even getting a partner, all involve sales.

`}
            />

            <CustomContentText
              label={`
              In a more professional context, a job interview, seeking a raise, pitching an idea, raising funds, negotiating with vendors, all involve sales. 
           `}
            />
            <CustomContentText
              label={`
              Every personal and professional relationship involves sales. In fact you even sell to yourself when you promise to go to the gym or eat healthy.

           `}
            />
            <CustomContentText
              label={`
             In a nutshell, everything is sales. And human beings can naturally sell. 

           `}
            />
            <CustomHeadingText
              label={"Why do people shy away from considering a sales job?"}
            />
            <CustomHeadingText label={"Never told about a sales career: "} />

            <CustomContentText
              label={` It's quite probable that this is the first time you are reading about pursuing a sales career. `}
            />
            <CustomContentText
              label={` By default, you may have explored or been directed towards a technical career as an engineer or doctor.
 `}
            />
            <CustomContentText
              label={` And even within more creative fields, design or marketing appear more glamorous.
 `}
            />
            <CustomContentText
              label={` There is hardly any mention of sales in schools or colleges, and you don’t really get to learn what sales is about. 
 `}
            />
            <CustomHeadingText label={"Limited & poor exposure to sales: "} />
            <CustomContentText
              label={`Perhaps your limited exposure to sales has been spam calls asking you to take loans you do not need. 

`}
            />
            <CustomContentText
              label={`Or that you think sales means going door to door. 
`}
            />
            <CustomContentText
              label={`Some salespeople might even engage in unethical sales practices. 

 

`}
            />
            <CustomContentText
              label={`Like anything, there are good and bad practices, and sales is no different. 

 

`}
            />
            <CustomContentText
              label={`The doctor prescribing additional procedures beyond what is necessary is also culpable. 

`}
            />
            <CustomContentText
              label={`One thing is certain, sales is absolutely essential, and powerful. 
 `}
            />
            <CustomContentText
              label={`In fact, the existence of poor sales practices is all the more reason to stand out with good practices. 

 `}
            />

            <CustomHeadingText
              label={" Sales is associated with risky entrepreneurship: "}
            />
            <CustomContentText
              label={` People think sales is about starting a business, which comes with high risks.`}
            />
            <CustomContentText
              label={`Well, you can always sell a solution in someone else’s business! `}
            />

            <CustomContentText
              label={`This is in fact a benefit of sales, where you get to build entrepreneurial and leadership skills to either start your own business in future, or be promoted to leadership positions. 
`}
            />

            <CustomHeadingText label={"So what really is sales?"} />

            <CustomContentText
              label={`In a nutshell, sales is about solving problems, communicating value, and guiding people to make informed decisions. 


`}
            />

            <CustomContentText
              label={`More complex the decision, the larger the role of a salesperson. 




`}
            />

            <CustomContentText
              label={` In consumer products, this can range from selling clothes to furniture to real estate. 




`}
            />

            <CustomContentText
              label={` 
                   In business settings, you have the sale of basic services to software to heavy machinery and complex technology.`}
            />

            <CustomContentText
              label={`Something such as investment banking is also largely sales, where they act as consultants to companies to raise millions and billions of dollars from investors.


`}
            />
            <CustomHeadingText label={"What’s special about a sales career?"} />
            <CustomHeadingText label={"Leadership Skills: "} />
            <CustomContentText
              label={` Sales is a great training ground for people to pick up problem solving and people skills, such as communication, negotiation, decision making, handling rejections, and collaboration. 


`}
            />
            <CustomContentText
              label={` This provides a platform to grow into managerial and leadership positions, or even start a business. 
`}
            />
            <CustomHeadingText label={"Connect with people: "} />
            <CustomContentText
              label={` Sales involves talking and dealing with people on a daily basis. This is great for who loves social interaction. 


`}
            />
            <CustomContentText
              label={` Even people who are typically introverts, can end up enjoying sales as it can involve deep and individual conversations, as opposed to public communication.

`}
            />
            <CustomHeadingText label={"Business Exposure & Impact: "} />
            <CustomContentText
              label={` Sales gives you an understanding of products, services, markets, and industries. 



`}
            />
            <CustomContentText
              label={` Being close to revenue, you understand how money is made, and can exert influence and create an impact. `}
            />

            <CustomHeadingText
              label={"Performance driven and rewarding career:  "}
            />
            <CustomContentText
              label={` Sales is typically a performance driven career, and rewarding for someone who performs. 



`}
            />
            <CustomContentText
              label={` It is also easier for you to see and measure the impact of your efforts and processes.  `}
            />
            <CustomHeadingText
              label={"In-demand and low competition career:  "}
            />
            <CustomContentText
              label={` Salespeople are high in demand, and many people are not really considering sales careers.




`}
            />
            <CustomContentText
              label={` If you already possess some sales skills, or some technical skills based on which you switch to a sales role within your specialised industry, getting into sales and growing from there is going to be a breeze. 

  `}
            />
            <CustomHeadingText
              label={
                "Who should get into sales? Who can get into sales? What are some sales career paths?  "
              }
            />
            <CustomContentText
              label={` If you ask us, everyone should do sales at least for some time in their life, for a transforming and enriching experience. 

`}
            />
            <CustomContentText
              label={` And sales is an accepting career, without high barriers to entry. You don’t need specialised knowledge or skills to make a start. 
  `}
            />
            <CustomContentText
              label={` Some scenarios are:


  `}
            />

            <CustomHeadingText
              label={"Low Technical Skills and Low / High People Skills: "}
            />
            <CustomContentText
              label={` If you fall under this category, it is likely that the technical fields are more stringent and less accepting of you, to give you a chance to learn and succeed.

`}
            />
            <CustomContentText
              label={` Sales on the other hand can be more accepting, and offer significant growth potential. 


  `}
            />
            <CustomContentText
              label={` Once you get in, your people skills can improve.




  `}
            />
            <CustomContentText
              label={`From hereon, you can be set for a seasoned sales career, and eventually grow into senior leadership positions.


`}
            />
            <CustomContentText
              label={` You can even switch industries freely, as companies value your people skills and unique perspectives.

  `}
            />
            <CustomContentText
              label={` Alternatively, you can switch to roles such as marketing, consulting, project management, supply chain management, and entrepreneurship. 



  `}
            />

            <CustomHeadingText
              label={"High Technical Skills and Low People Skills:"}
            />
            <CustomContentText
              label={`
              If you are to continue pursuing a technical career, in most cases, you might see growth stagnate after some initial progress. `}
            />
            <CustomContentText
              label={` In this case we’d recommend pursuing a short sales stint. You might especially be a fit to sell complex and technical products, aka a sales engineer, a highly sought after role. `}
            />
            <CustomContentText
              label={` Even with products which are not highly technical, your technical prowess can help you stand out and accelerate your sales career.`}
            />
            <CustomContentText
              label={`This can level up your people and business skills to make you an all-round techno-commercial person, and give you high leverage and open up lots of possibilities.
             `}
            />
            <CustomContentText
              label={` If you are to return to a technical role, you’d be better equipped to grow into leadership positions.   `}
            />
            <CustomContentText
              label={` Alternatively, you can opt for techno-commercial roles, such as product management, consulting, and operations. 




  `}
            />
            <CustomContentText
              label={`And if you end up enjoying sales, you can stick to it and become a sales superpower within your specialised industry. 
`}
            />

            <CustomHeadingText
              label={"High Technical Skills and Low People Skills:"}
            />
            <CustomContentText
              label={`
              
                   In this case you are already all set to grow out of a technical role and transition into a leadership role, for which sales can be an ideal grooming platform.
  `}
            />
            <CustomContentText
              label={` After a short stint in sales, you can get into leading a technical team, techno-commercial roles, or carry on with sales as a superstar, even within different industries.
 `}
            />

            <CustomHeadingText label={"Bonus:"} />
            <CustomContentText
              label={`
              If you are from a sports or esports background, you already have a sales mindset, and it could be a great fit for you.`}
            />
            <CustomContentText
              label={`You probably know that it is not “win or learn”, but rather about continuous improvement and learning from both wins and losses.
 `}
            />
            <CustomContentText
              label={`
                  You know how to deal with losses. You can’t win every game, but you can’t let losses affect the next game to play. If you do things right, you’ll win more than you lose. 
  `}
            />
            <CustomContentText
              label={` And a lot more. 

 `}
            />

            <CustomHeadingText
              label={"Sounds interesting. How do I get into sales? "}
            />
            <CustomContentText
              label={`
              That’s what Sales Jobverse is for! 
`}
            />
            <CustomContentText
              label={`At the moment, you can use our platform to discover sales jobs. 

 `}
            />
            <CustomContentText
              label={`
                 
     We’re on the works to bring you more value with exciting features. Meanwhile, you can follow our social media for updates and some valuable content. 

  `}
            />
            <CustomContentText
              label={`And ultimately, the best way, by far, to land a sales job is to send a video introduction. 


 `}
            />
            <CustomContentText
              label={`
                 
       So simply record yourself speaking, improve your video introduction, and you’d be a shoe in for multiple interviews and offers. 
`}
            />
            <CustomContentText
              label={`And once you secure a job, you can get groomed on the job and reap the rewards of a sales career. 

 `}
            />
          </Box>
        </Box>
      </ScrollArea>
    </Box>
  );
};

export default AboutDatapage;
