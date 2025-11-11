import SectionContainer from "@components/SectionContainer";
import { PaperProps } from "@mantine/core";

const EmployerHomePageCard = ({
  children,
  style,
  ...props
}: {
  children: React.ReactNode;
  style?: Record<string, any>;
} & PaperProps) => {
  return (
    <SectionContainer
      style={{
        position: "relative",
        overflow: "visible",
        width: "100%",
        ...style,
      }}
      {...props}
    >
      {children}
    </SectionContainer>
  );
};
export default EmployerHomePageCard;
