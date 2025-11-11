import Link from "next/link";
import { Text, TextProps } from "@mantine/core";
import { useState } from "react";
const CustomLink = ({
  label,
  href,
  ...props
}: { label: string; href: string } & TextProps) => {
  const [isHovered, setIsHovered] = useState<boolean>(false);
  return (
    <Link href={href} style={{ textDecoration: "none" }}>
      <Text
        onMouseEnter={() => setIsHovered(true)}
        onMouseLeave={() => setIsHovered(false)}
        c={isHovered ? "primarySkyBlue.6" : "white"}
        fz={{ base: 14, xl: 18 }}
        fw="500"
        lh="1.17"
        {...props}
      >
        {label}
      </Text>
    </Link>
  );
};
export default CustomLink;
