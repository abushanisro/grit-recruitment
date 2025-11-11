import { JobseekerDirectoryFilterInterface } from "@/types/jobSeeker";
import { getTimeFromNow } from "@/utils/common";
import CustomHeadingText from "@components/CustomHeadingText";
import AsyncSearchSelectField from "@components/form/AsyncSearchSelectField";
import { Flex, Stack, Image } from "@mantine/core";
import { useMediaQuery } from "@mantine/hooks";
import { getQueryClient } from "api";

import isEmpty from "lodash/isEmpty";
import { UseFormReturn } from "react-hook-form";

const SubscriptionFilter = ({
  hForm,
}: {
  hForm: UseFormReturn<JobseekerDirectoryFilterInterface>;
}) => {
  const {
    setValue,
    formState: { isDirty },
  } = hForm;
  const laptop = useMediaQuery("(max-width:992px)");
  const mobile = useMediaQuery("(max-width:525px)");
  const tablet = useMediaQuery("(max-width:767px)");

  return (
    <Flex
      justify="space-between"
      mt={mobile ? 10 : laptop ? 40 : 0}
      mb={{ base: 0, sm: 40 }}
    >
      <CustomHeadingText
        display={{ base: "none", sm: "block" }}
        label={"Jobseeker Directory"}
      />

      <Stack
        gap={2}
        w={tablet ? "100%" : "max-content"}
        align={tablet ? "none" : "center"}
      >
        <CustomHeadingText
          fz={{ base: 18, md: 20 }}
          c="secondaryGreen.1"
          label={"Selected Subcriptions"}
        />
        <AsyncSearchSelectField
          hForm={hForm}
          name="activeSubscription"
          placeholder="subscriptions"
          rules={{ required: "plan is required" }}
          isClearable={false}
          getOptions={async (val: string) => {
            const data =
              await getQueryClient().subscription.suggestionActiveSubscriptions.query();
            if (data.status === 200) {
              if (!isDirty && !isEmpty(data)) {
                setValue("activeSubscription", {
                  label: `${data.body[0].name} will expire  ${getTimeFromNow(
                    data.body[0].expiryDate
                  )}`,
                  value: data.body[0].id.toString(),
                });
              }

              return data.body.map((eachValue) => ({
                label: `${eachValue.name} ( will expire  ${getTimeFromNow(
                  eachValue.expiryDate
                )} )`,
                value: eachValue.id.toString(),
              }));
            }

            return [];
          }}
          instanceId="activeSubscription"
          customStyles={{
            control: () => ({
              fontSize: laptop ? 14 : 16,
              backgroundColor: "var(--mantine-color-primarySkyBlue-6)",
              paddingInline: tablet ? 0 : 40,
            }),
          }}
          dropDownIcon={
            <Image
              src="/images/downArrow.svg"
              w={10}
              h={8}
              alt="indicator-icon"
            />
          }
        />
      </Stack>
    </Flex>
  );
};

export default SubscriptionFilter;
