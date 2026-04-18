import axios from "axios";

export async function fetchFourMemeData(tokenAddress) {
  try {
    const response = await axios.get(
      `${process.env.FOUR_MEME_API}?address=${tokenAddress}`
    );

    return response.data;
  } catch (err) {
    console.error("API error:", err.message);
    return null;
  }
}