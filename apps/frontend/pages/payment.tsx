import Head from "next/head";
import { useQueryState } from "@/hooks/queryState";
import PageLayout from "@components/layouts/PageLayout";
import Navbar from "@components/Navbar";
import { Box, Group, Text } from "@mantine/core";
import isNil from "lodash/isNil";
import { useMediaQuery } from "@mantine/hooks";

import MyPaymentList from "@components/tableComponents/MyPaymentList";
import SectionTableContainer from "@components/SectionTableContainer";
import { ActiveTableTabButton } from "@components/buttons/ActiveTableTabButton";
import PaymentPostPaid from "@components/tableComponents/PaymentPostPaid";

const PaymentPage = () => {
  return (
    <>
      <Head>
        <title>My Payment | Jobverse</title>
        <meta
          name="description"
          content="Sales Jobverse - My Payment and Subscription"
        />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <link rel="icon" href="/favicon.ico" />
      </Head>
      <PageLayout
        navbarComponent={<Navbar />}
        mainComponent={<MyPaymentPage />}
      />
    </>
  );
};
export enum TabsEnum {
  payment = "Job Seeker Directory",
  postPaid = "Paid Job Post",
}
const MyPaymentPage = () => {
  const isTablet = useMediaQuery("(max-width: 767px)");
  const [activeTab, setActiveTab] = useQueryState<string | null>(
    "activeTab",
    TabsEnum.payment
  );

  if (isNil(activeTab) || !activeTab) {
    return <></>;
  }
  return (
    <Box mt={{ base: 0, md: 30 }}>
      <Text fz={{ base: 28, sm: 32 }} fw={600} style={{ letterSpacing: "2px" }}>
        My Payments
      </Text>

      <Group
        justify="flex-start"
        wrap="nowrap"
        pl={{ base: 0, sm: 40, md: 80 }}
        mt={{ base: 12, md: 24 }}
        py={isTablet ? 16 : 0}
        style={{
          overflowX: "scroll",
        }}
      >
        {Object.values(TabsEnum).map((applicationStatus, index) => {
          const isActive = activeTab === applicationStatus;
          return (
            <ActiveTableTabButton
              key={index}
              isActive={isActive}
              label={applicationStatus}
              onClick={() => {
                setActiveTab(applicationStatus);
              }}
            />
          );
        })}
      </Group>

      <SectionTableContainer>
        {activeTab === TabsEnum.payment ? (
          <MyPaymentList />
        ) : (
          <PaymentPostPaid />
        )}
      </SectionTableContainer>
    </Box>
  );
};

export default PaymentPage;
